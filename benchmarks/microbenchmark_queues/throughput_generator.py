import argparse
import base64
import json
import socket
import sys
import time
import urllib
from datetime import datetime, timedelta
from functools import partial

from google.cloud import pubsub_v1
from google.auth.transport.requests import Request
import requests
import google.oauth2.id_token

import pandas as pd
import boto3

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument('--region', type=str)
parser.add_argument('--output-prefix', type=str)
parser.add_argument('--rps', type=int)
parser.add_argument('--repetitions', type=int)
parser.add_argument('--memory', type=int)
parser.add_argument('--queue', type=str)
parser.add_argument('--size', type=int)
parser.add_argument('--queue-name', type=str)
parser.add_argument('--message-id', type=int)
parser.add_argument('--fname', type=str)
parser.add_argument('--port', type=int)
args = parser.parse_args()

sqs_client = boto3.client('sqs', region_name=args.region)
lambda_client = boto3.client('lambda', region_name=args.region)
dynamo_client = boto3.client('dynamodb', region_name=args.region)
sqs_queue_url = None

# the batch will be sent if any of the setting is met.
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=10,  # default 100, now it is 10
    max_bytes= 1 * 1000 * 1000,  # default 1 MB, still 1 MB -> 1000 * 1000 KB
    max_latency=0.0001,  # default 10 ms, now is .1ms
)

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True) # enable FIFO
publisher_client = pubsub_v1.PublisherClient(publisher_options=publisher_options, batch_settings= batch_settings)
_fifo_topic_id = "benchmark"
_project_id = "wide-axiom-402003"
fifo_topic_path = publisher_client.topic_path(_project_id, _fifo_topic_id)
_topic_id = "benchmark2"
topic_path = publisher_client.topic_path(_project_id, _topic_id)

# cloud function direct
endpoint = f"https://us-central1-{_project_id}.cloudfunctions.net/throughput"

"""
    This benchmark evaluates the time needed to schedule and execute a serverless
    function from a queue item.
    We support the following queue types:
    - SQS
    - SQS FIFO
    - SQS FIFO HighThroughput
    - DynamoDB streams
"""

# in bytes
# base64 encoded string begins at 4 bytes anyway
#BENCHMARK_SIZES = [64, 1024, 32*1024, 64*1024, 128*1024]
#MEMORY_SIZES = [128, 512, 1024, 2048]
BENCHMARK_SIZES = [64, 1024, 32*1024, 64*1024, 128*1024]
MEMORY_SIZES = [128, 512, 1024, 2048]

def generate_binary_data(size):
    # for n bytes the length of base64 string is 4 * n / 3 (unpadded)
    # then it's padded to 4 bytes
    # so the reverse is: n * 3/4 - we always select multiples of fours
    original_size = int(size * 3 / 4)
    return base64.b64encode(bytearray([1] * original_size))
    #return bytes(bytearray([1]*original_size))

def prepare_socket():

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", args.port))

    req = urllib.request.urlopen("https://checkip.amazonaws.com")
    addr = req.read().decode().strip()
    port = sock.getsockname()[1]

    sock.settimeout(0.5)
    sock.listen(1)

    return sock, addr, port

def test_pubsub_fifo(fifo_topic_name, data, _, addr, port):
    # push subs is one by one
    payload = {
        "ip": f"{addr}",
        "port": port,
        "data": data.decode()
    }
    data = json.dumps(payload).encode("utf-8")
    publisher_client.publish(fifo_topic_name, data=data, ordering_key= "0")

def test_pubsub(topic_name, data, _, addr, port):
    payload = {
        "ip": f"{addr}",
        "port": port,
        "data": data.decode()
    }
    data = json.dumps(payload).encode("utf-8")
    publisher_client.publish(topic_name, data=data, ordering_key= "0")

def test_lambda(func_name, data, msg_id, addr, port):
    body = json.dumps({
        'Records': [{
            "ip": f"{addr}",
            "port": port,
            "data": data.decode()
        }]
    })
    #print(f"Send data len {len(data)}")
    lambda_client.invoke(FunctionName=func_name, InvocationType='Event', Payload=body.encode('utf-8'))

def test_sqs(queue_url, data, msg_id, addr, port):
    body = json.dumps({
        "ip": f"{addr}",
        "port": port
    })
    #print(f"Send data len {len(data)}")
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        MessageAttributes={
            'body': {
                'BinaryValue': data,
                'DataType': 'Binary'
            }
        }
    )
    return response['MessageId']

def test_sqs_fifo(queue_url, data, msg_id, addr, port):
    body = json.dumps({
        "ip": f"{addr}",
        "port": port
    })
    #print(f"Send data len {len(data)}")
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        MessageAttributes={
            'body': {
                'BinaryValue': data,
                'DataType': 'Binary'
            }
        },
        MessageGroupId='0',
        MessageDeduplicationId=str(msg_id)
    )
    return msg_id
import uuid
def test_dynamo(queue_url, data, msg_id, addr, port):
    body = json.dumps({
        "ip": f"{addr}",
        "port": port
    })
    #print(f"Send data len {len(data)}")
    timestamp = f"{str(uuid.uuid4())[0:8]}"
    data = {
        "ip": {'S': f"{addr}"},
        "port": {'S': f"{port}"},
        "data": {'B': data},
        "timestamp": {'S': timestamp},
        "key": {'S': "BENCHMARK"}
    }
    dynamo_client.put_item(                                            
        TableName=queue_url,
        Item=data,
    )
    return timestamp

req = urllib.request.urlopen("https://checkip.amazonaws.com")
addr = req.read().decode().strip()
COMMUNICATION_REPS = 100
dfs = []
queue_message_id = args.message_id

#memory = args.memory
#print(f"Update config to {memory}")
#lambda_client.update_function_configuration(
#    FunctionName=args.fname, MemorySize=memory
#)
#print("Done")
#time.sleep(10)
#print("Begin")

if args.queue in ['sqs', 'sqs_fifo']:
    response = sqs_client.get_queue_url(
        QueueName=args.queue_name
    )
    sqs_queue_url = response['QueueUrl']
elif args.queue == 'lambda':
    func_name = args.queue_name
else:
    dynamo_table = args.queue_name

test_func = None
if args.queue == 'sqs_fifo':
    test_func = partial(test_sqs_fifo, sqs_queue_url)
elif args.queue == 'sqs':
    test_func = partial(test_sqs, sqs_queue_url)
elif args.queue == 'dynamo':
    test_func = partial(test_dynamo, dynamo_table)
elif args.queue == 'lambda':
    test_func = partial(test_lambda, func_name)
elif args.queue == 'pubsub_fifo':
    test_func = partial(test_pubsub_fifo, fifo_topic_path) # dummy function
elif args.queue == 'pubsub':
    test_func = partial(test_pubsub, topic_path)

print(f'Begin benchmarking invocations with the queue {args.queue}')

results = []
size = args.size
data = generate_binary_data(args.size)
print(f"Start repetitions with size {size}, data len {len(data)}")
total_begin = time.time()# datetime.now()

period = 1.0/args.rps
repetitions = args.rps * args.repetitions
start = time.time()
for i in range(repetitions):
    #begin = time.time()
    #time.sleep(0.001)
    _id = test_func(data, queue_message_id, addr, args.port)
    #print("sent")
    end = time.time()
    results.append([end, _id])
    queue_message_id += 1
    delta = start + period * (i +1) - time.time()
    if delta > 0:
        time.sleep(delta)

total_end = time.time()#datetime.now()
print(f"Send {repetitions}")

#for val in results:
#    val[0] = val[0].timestamp()

results.append([total_begin, -1])
results.append([total_end, -2])
df_timing = pd.DataFrame(data=results, columns=["time", "idx"])
df_timing["queue"] = args.queue
df_timing["memory"] = args.memory
df_timing["size"] = size
df_timing["type"] = 'send'

df_timing.to_csv(f"{args.output_prefix}.csv")


