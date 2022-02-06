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

    sock.settimeout(10)
    sock.listen(1)

    return sock, addr, port


results = []

sock, addr, port = prepare_socket()
print(f"Addr: {addr}, Port: {port}")

timing_results = []
for i in range(args.repetitions):
    try:
        conn, addr = sock.accept()
        #a = 1
    except socket.timeout:
        print("Timeout")
        continue
        #pass
    except Exception as e:
        raise e
    else:
        #data = json.loads(conn.recv(64).decode())
        data = conn.recv(128)
        #print(data)
        #data = b'1'
        end = time.time()
        timing_results.append([end, data])

for i in range(len(timing_results)):
    try:
        data = json.loads(timing_results[i][1].decode())
        timing_results[i] = [timing_results[i][0], data['idx'], data['events'], data['is_cold']]
    except json.decoder.JSONDecodeError:
        break

print(f"Received {args.repetitions}")

df_timing = pd.DataFrame(data=timing_results, columns=["timestamp", "idx", "events", "is_cold"])
df_timing["queue"] = args.queue
df_timing["memory"] = args.memory
df_timing["type"] = 'rtt'
df_timing["size"] = args.size
df_timing.to_csv(f"{args.output_prefix}.csv")


