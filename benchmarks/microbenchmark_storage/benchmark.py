
import base64
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import boto3
from boto3.dynamodb.types import Binary


region = sys.argv[1]
output_prefix = sys.argv[2]
repetitions = int(sys.argv[3])
storage_type = sys.argv[4]
cleanup = sys.argv[5].lower() == 'true'
max_size = int(sys.argv[6]) if len(sys.argv) == 7 else -1

# in bytes
# base64 encoded string begins at 4 bytes anyway
BENCHMARK_SIZES = [2**i for i in range(2, 20)]
DYNAMODB_TABLE_NAME = "faaskeeper_microbenchmark_storage"

def generate_binary_data(size):
    # for n bytes the length of base64 string is 4 * n / 3 (unpadded)
    # then it's padded to 4 bytes
    # so the reverse is: n * 3/4 - we always select multiples of fours
    original_size = int(size * 3 / 4)
    return base64.b64encode(bytearray([1] * original_size))

def test_s3_read():
    pass

def test_s3_write():
    pass

def test_dynamodb_read(client, name, size, repetitions):

    results = []
    read_capacity = 0.0
    for i in range(repetitions):
        begin = datetime.now()
        ret = client.get_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={
                "key": {
                    'S': name
                },
            },
            ReturnConsumedCapacity='TOTAL',
            ConsistentRead=True
        )
        read_capacity += ret['ConsumedCapacity']['CapacityUnits']
        end = datetime.now()
        results.append(int((end - begin) / timedelta(microseconds=1)))
    print(f"Expected read size {size}, output data size: {len(ret['Item']['data']['B'])}")
    return results, read_capacity

def test_dynamodb_write(client, name, size, repetitions):

    results = []
    data = generate_binary_data(size)
    print(f"Expected write size {size}, input data size: {len(data)}")
    write_capacity = 0.0
    for i in range(repetitions):
        begin = datetime.now()
        ret = client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            Item={
                "key": {
                    'S': name
                },
                "data": {
                    'B': data
                }
            },
            ReturnConsumedCapacity='TOTAL'
        )
        write_capacity += ret['ConsumedCapacity']['CapacityUnits']
        end = datetime.now()
        results.append(int((end - begin) / timedelta(microseconds=1)))
    return results, write_capacity


def init_s3():
    pass

def delete_s3(client):
    pass

def init_dynamodb(region):
    client = boto3.client("dynamodb")

    # Testing table: string key, binary data
    try:
        ret = client.create_table(
            AttributeDefinitions=[
                {
                'AttributeName': 'key',
                'AttributeType': 'S'
                }
            ],
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'key',
                    'KeyType': 'HASH'
                },
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print("Sleep to make sure the table is up and running")
        time.sleep(5)
    except client.exceptions.ResourceInUseException:
        print("Table already exists")
    return client


def delete_dynamodb(client):
    client.delete_table(TableName=DYNAMODB_TABLE_NAME)

print(f"Initialize {storage_type}")
if storage_type == 's3':
    client = init_s3()
elif storage_type == 'dynamodb':
    client = init_dynamodb(region)
else:
    raise NotImplementedError()

dfs = []
for size in BENCHMARK_SIZES:

    # evaluate at the maximum
    if max_size != -1 and size > max_size:
        size = max_size
    print(f"Testing write data of size {size}")
    results, write_capacity = test_dynamodb_write(client, f"size_{size}", size, repetitions)
    df_write = pd.DataFrame(data=results, columns=["data"])
    df_write['op'] = 'write'
    df_write = df_write.append({'data': write_capacity, 'op': 'write_capacity'}, ignore_index=True)
    df_write['storage'] = 'dynamodb'
    df_write['size'] = size
    dfs.append(df_write)

    print(f"Testing read data of size {size}")
    results, read_capacity = test_dynamodb_read(client, f"size_{size}", size, repetitions)
    df_read = pd.DataFrame(data=results, columns=["data"])
    df_read['op'] = 'read'
    df_read['storage'] = 'dynamodb'
    df_read = df_read.append({'data': read_capacity, 'op': 'read_capacity'}, ignore_index=True)
    df_read['size'] = size
    dfs.append(df_read)

    if max_size != -1 and size > max_size:
        break
    break

df = pd.concat(dfs, axis=0, ignore_index = True)
df.to_csv(f"{output_prefix}.csv")

if cleanup:
    print(f"Clean {storage_type}")
    if storage_type == 's3':
        delete_s3(client)
    elif storage_type == 'dynamodb':
        delete_dynamodb(client)
    else:
        raise NotImplementedError()

