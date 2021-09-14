import argparse
import base64
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import boto3

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument('--region', type=str)
parser.add_argument('--output-prefix', type=str)
parser.add_argument('--repetitions', type=int)
parser.add_argument('--cleanup', action='store_true', default=False)
parser.add_argument('--max-size', type=int, default=-1)
args = parser.parse_args()

# in bytes
# base64 encoded string begins at 4 bytes anyway
BENCHMARK_SIZES = [64, 64*1024]
ATOMIC_SIZES = [1, 128, 1024]
LIST_SIZES = [1, 128, 1024]
ITEM_NAMES ={
    "timelock_acquire": 'dynamodb-timelock-acquire-test',
    "timelock_release": 'dynamodb-timelock-release-test',
    "atomic_counter": 'dynamodb-atomic-counter-test',
    "atomic_list": 'dynamodb-atomic-list-test'
}
DYNAMODB_TABLE_NAME = "faaskeeper_microbenchmark_primitives"

def generate_binary_data(size):
    # for n bytes the length of base64 string is 4 * n / 3 (unpadded)
    # then it's padded to 4 bytes
    # so the reverse is: n * 3/4 - we always select multiples of fours
    original_size = int(size * 3 / 4)
    return base64.b64encode(bytearray([1] * original_size))

def test_timelock_acquire(client, name, size, repetitions):
    results = []
    path = ITEM_NAMES.get('timelock_acquire')
    data = generate_binary_data(size)
    client.put_item(
        TableName=DYNAMODB_TABLE_NAME,
        # path to the node
        Item={"key": {"S": name}, "data": {"B": data}},
        ReturnConsumedCapacity="TOTAL",
    )
    read_capacity = 0.0
    lock_lifetime = 7
    for i in range(repetitions+1):
        begin = datetime.now()
        # acquire lock
        timestamp = int(datetime.now().timestamp())
        ret = client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Key={"key": {"S": name}},
            # create timelock
            UpdateExpression="SET timelock = :newlockvalue",
            # lock doesn't exist or it's already expired
            ConditionExpression="(attribute_not_exists(timelock)) or "
            "(timelock < :newlockshifted)",
            # timelock value
            ExpressionAttributeValues={
                 ":newlockvalue": {"N": str(timestamp)},
                 ":newlockshifted": {"N": str(timestamp - lock_lifetime)},
            },
            ReturnValues="ALL_NEW",
            ReturnConsumedCapacity="TOTAL",
        )
        end = datetime.now()

        # remove lock
        client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Key={"key": {"S": name}},
            # create timelock
            UpdateExpression="REMOVE timelock",
            ReturnValues="ALL_NEW",
            ReturnConsumedCapacity="TOTAL",
        )

        if i == 0:
            continue
        read_capacity += ret["ConsumedCapacity"]["CapacityUnits"]
        results.append(int((end - begin) / timedelta(microseconds=1)))


    print(f"Expected read size {size}, output data size: {len(data)}")
    return results, repetitions

def test_timelock_release(client, name, size, repetitions):
    results = []
    path = ITEM_NAMES.get('timelock_release')
    data = generate_binary_data(size)
    client.put_item(
        TableName=DYNAMODB_TABLE_NAME,
        # path to the node
        Item={"key": {"S": name}, "data": {"B": data}},
        ReturnConsumedCapacity="TOTAL",
    )
    read_capacity = 0.0
    lock_lifetime = 7
    timestamp = 100
    for i in range(repetitions+1):
        client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Key={"key": {"S": name}},
            # create timelock
            UpdateExpression="SET timelock = :newlockvalue",
            # timelock value
            ExpressionAttributeValues={
                 ":newlockvalue": {"N": str(timestamp)},
            },
            ReturnConsumedCapacity="TOTAL",
        )
        begin = datetime.now()
        # remove lock
        ret = client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Key={"key": {"S": name}},
            # create timelock
            UpdateExpression="REMOVE timelock",
            # lock doesn't exist or it's already expired
            ConditionExpression="(attribute_exists(timelock)) and (timelock = :mytimelock)",
            # timelock value
            ExpressionAttributeValues={
                 ":mytimelock": {"N": str(timestamp)},
            },
            ReturnValues="NONE",
            ReturnConsumedCapacity="TOTAL",
        )
        end = datetime.now()


        if i == 0:
            continue
        read_capacity += ret["ConsumedCapacity"]["CapacityUnits"]
        results.append(int((end - begin) / timedelta(microseconds=1)))


    print(f"Expected read size {size}, output data size: {len(data)}")
    return results, repetitions

def test_atomic_counter(client, name, size, repetitions):
    results = []
    path = ITEM_NAMES.get('atomic_counter')
    data = generate_binary_data(size)
    counter = [{"N": "0"}]*size
    client.put_item(
        TableName=DYNAMODB_TABLE_NAME,
        # path to the node
        Item={"key": {"S": name}, "data": {"L": counter}},
        ReturnConsumedCapacity="TOTAL",
    )
    read_capacity = 0.0
    lock_lifetime = 7
    timestamp = 100
    pos = int(size / 2)
    for i in range(repetitions+1):
        begin = datetime.now()
        ret = client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Key={"key": {"S": name}},
            UpdateExpression=f"ADD #D[{pos}] :inc",
            ExpressionAttributeNames={"#D": "data"},
            ExpressionAttributeValues={":inc": {"N": "1"}},
            ReturnValues="ALL_NEW",
            ReturnConsumedCapacity="TOTAL",
        )
        end = datetime.now()
        if i == 0:
            continue
        read_capacity += ret["ConsumedCapacity"]["CapacityUnits"]
        results.append(int((end - begin) / timedelta(microseconds=1)))


    print(f"Expected read size {size}, output data size: {len(data)}")
    return results, repetitions

def test_atomic_list_increase(client, name, size, repetitions):
    results = []
    path = ITEM_NAMES.get('atomic_list')
    _data = generate_binary_data(64)
    data = {"L": [{"B": _data}]*size}
    read_capacity = 0.0
    for i in range(repetitions+1):
        client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Item={"key": {"S": name}, "list_counter": data},
            ReturnConsumedCapacity="TOTAL",
        )
        begin = datetime.now()
        ret = client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            # path to the node
            Key={"key": {"S": name}},
            UpdateExpression=f"SET list_counter = list_append(if_not_exists(list_counter, :empty_list), :newItem)",
            ExpressionAttributeValues={":newItem": {"L": [{"B": _data}]}, ":empty_list": {"L": []}},
            ReturnValues="ALL_NEW",
            ReturnConsumedCapacity="TOTAL",
        )
        end = datetime.now()
        if i == 0:
            continue
        read_capacity += ret["ConsumedCapacity"]["CapacityUnits"]
        results.append(int((end - begin) / timedelta(microseconds=1)))


    print(f"Expected read size {size}, output data size: {len(data)}")
    return results, repetitions

def test_atomic_list_truncate(client, name, size, repetitions):
    pass

def init_dynamodb(region):
    client = boto3.client("dynamodb", region_name=region)

    # Testing table: string key, binary data
    try:
        client.create_table(
            AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print("Sleep to make sure the table is up and running")
        time.sleep(10)
    except client.exceptions.ResourceInUseException:
        print("Table already exists")
    return client


def delete_dynamodb(client):
    client.delete_table(TableName=DYNAMODB_TABLE_NAME)


print(f"Initialize")
client = init_dynamodb(args.region)

dfs = []
for size in BENCHMARK_SIZES:

    # evaluate at the maximum
    if args.max_size != -1 and size > args.max_size:
        size = args.max_size
    print(f"Testing lock acquire of size {size}")
    results, write_capacity = test_timelock_acquire(client, f"size_{size}", size, args.repetitions)
    df_write = pd.DataFrame(data=results, columns=["data"])
    df_write["op"] = "timelock_acquire"
    df_write = df_write.append({"data": write_capacity, "op": "timelock_acquire_capacity"}, ignore_index=True)
    df_write["storage"] = "dynamodb"
    df_write["size"] = size
    dfs.append(df_write)

    print(f"Testing lock release data of size {size}")
    results, write_capacity = test_timelock_release(client, f"size_{size}", size, args.repetitions)
    df_write = pd.DataFrame(data=results, columns=["data"])
    df_write["op"] = "timelock_release"
    df_write = df_write.append({"data": write_capacity, "op": "timelock_release_capacity"}, ignore_index=True)
    df_write["storage"] = "dynamodb"
    df_write["size"] = size
    dfs.append(df_write)

    if args.max_size != -1 and size > args.max_size:
        break

for length in ATOMIC_SIZES:
    print(f"Testing atomic increment of length {length}")
    results, write_capacity = test_atomic_counter(client, f"size_{length}", length, args.repetitions)
    df_write = pd.DataFrame(data=results, columns=["data"])
    df_write["op"] = "atomic_counter"
    df_write = df_write.append({"data": write_capacity, "op": "atomic_counter_capacity"}, ignore_index=True)
    df_write["storage"] = "dynamodb"
    df_write["size"] = length
    dfs.append(df_write)

for length in ATOMIC_SIZES:
    print(f"Testing atomic list increment of length {length}")
    results, write_capacity = test_atomic_list_increase(client, f"size_{length}", length, args.repetitions)
    df_write = pd.DataFrame(data=results, columns=["data"])
    df_write["op"] = "atomic_list_increase"
    df_write = df_write.append({"data": write_capacity, "op": "atomic_list_increase_capacity"}, ignore_index=True)
    df_write["storage"] = "dynamodb"
    df_write["size"] = length
    dfs.append(df_write)

df = pd.concat(dfs, axis=0, ignore_index=True)
df.to_csv(f"{args.output_prefix}.csv")

if args.cleanup:
    print(f"Clean")
    delete_dynamodb(client)

