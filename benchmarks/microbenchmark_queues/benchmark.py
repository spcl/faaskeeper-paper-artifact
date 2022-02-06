import argparse
import base64
import json
import socket
import sys
import time
import urllib
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
import boto3

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument('--region', type=str)
parser.add_argument('--output-prefix', type=str)
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
sqs_queue_url = None

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
    #return base64.b64encode(bytearray([1] * original_size))
    return bytes(bytearray([1]*original_size))

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

def test_sqs_fifo(queue_url, data, msg_id, addr, port):
    body = json.dumps({
        "ip": f"{addr}",
        "port": port
    })
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

try:
    conn, addr = sock.accept()
except socket.timeout:
    pass
except Exception as e:
    raise e
else:
    print('Connected, beginning RTT measurement')

if args.queue in ['sqs', 'sqs_fifo']:
    response = sqs_client.get_queue_url(
        QueueName=args.queue_name
    )
    sqs_queue_url = response['QueueUrl']

test_func = None
if args.queue == 'sqs_fifo':
    test_func = partial(test_sqs_fifo, sqs_queue_url)

print(f'Begin benchmarking invocations with the queue {args.queue}')

results = []
size = args.size
data = generate_binary_data(args.size)
print(f"Start repetitions with size {size}")
total_begin = datetime.now()
for i in range(args.repetitions):
    begin = datetime.now()
    #test_func(data, queue_message_id, addr, port)
    print("sent")
    end = datetime.now()
    #results.append([int((end - begin) / timedelta(microseconds=1)), i])
    results.append([end.timestamp(), i])
    queue_message_id += 1
total_end = datetime.now()
results.append([total_begin, -1])
results.append([total_begin, -2])
df_timing = pd.DataFrame(data=results, columns=["time", "idx"])
df_timing["queue"] = args.queue
df_timing["memory"] = args.memory
df_timing["size"] = size
df_timing["type"] = 'send'

df_timing.to_csv(f"{args.output_prefix}.csv")


