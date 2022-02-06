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
parser.add_argument('--queue', type=str)
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
BENCHMARK_SIZES = [64, 1024, 32*1024, 64*1024, 128*1024]
MEMORY_SIZES = [128, 512, 1024, 2048]
#BENCHMARK_SIZES = list(range(1, 100), 10)
#MEMORY_SIZES = [128, 512, 1024, 2048]

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

def test_sqs(queue_url, data, msg_id, addr, port):
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
        }
    )

COMMUNICATION_REPS = 100
queue_message_id = args.message_id

def run_test(idx, memory, message_id, queue, processes, size, repetitions):

    test_func = None
    queue_message_id = message_id + idx * repetitions * len(BENCHMARK_SIZES) * BENCHMARK_SIZES[-1]
    if queue == 'sqs_fifo':
        test_func = partial(test_sqs_fifo, sqs_queue_url)

    data = generate_binary_data(size)
    df_timing = pd.DataFrame(data=timing_results, columns=["data"])
    df_timing["process"] = idx
    df_timing["queue"] = args.queue
    df_timing["memory"] = memory
    df_timing["type"] = 'rtt'
    dfs.append(df_timing)
    print(f'Begin benchmarking invocations with the queue {args.queue}')
    results = []
    barrier = multiprocessing.Barrier(parties=processes)
    sock, addr, port = prepare_socket()


    for size in BENCHMARK_SIZES:


        for i in range(args.repetitions):

            timing_results = []
            print(f"Start repetition {i} with size {size}")
            barrier.wait()
            # send multiple copies
            begin = datetime.now()
            for j in range(size):
                test_func(data, queue_message_id, addr, port)
                queue_message_id += 1

            for j in range(size):
                try:
                    conn, addr = sock.accept()
                except socket.timeout:
                    pass
                except Exception as e:
                    raise e
                else:
                    print('Connected, beginning RTT measurement')
                    data = conn.recv(32)
                    end = datetime.now()
                    timing_results.append(int((end - begin) / timedelta(microseconds=1)))
            for i in range(COMMUNICATION_REPS):
                begin = datetime.now()
                conn.sendall(b'0')
                data = conn.recv(32)
                end = datetime.now()
            print('Finished RTT measurement')
            break
            sock.accept()
            data = conn.recv(1024)
            end = datetime.now()
            ret = json.loads(data.decode())
            results.append([int((end - begin) / timedelta(microseconds=1)), ret['is_cold']])

            if i % 10 == 0:
                print(f"Conducted {i} repetitions out of {args.repetitions}")
        df_invoc = pd.DataFrame(data=results, columns=["data", "is_cold"])
        df_invoc["queue"] = args.queue
        df_invoc["size"] = int(size)
        df_invoc["memory"] = memory
        df_invoc["type"] = 'invocation'
        dfs.append(df_invoc)

dfs = []

for memory in MEMORY_SIZES:

    print(f"Update config to {memory}")
    lambda_client.update_function_configuration(
        FunctionName=args.fname, MemorySize=memory
    )
    print("Done")
    time.sleep(10)
    print("Begin")

    if args.queue in ['sqs', 'sqs_fifo']:
        response = sqs_client.get_queue_url(
            QueueName=args.queue_name
        )
        sqs_queue_url = response['QueueUrl']

    test_func = None
    if args.queue == 'sqs_fifo':
        test_func = partial(test_sqs_fifo, sqs_queue_url)
    elif args.queue == 'sqs':
        test_func = partial(test_sqs, sqs_queue_url)


    print('Connect to the serverless worker.')
    sock, addr, port = prepare_socket()
    test_func(b'0', queue_message_id, addr, port)
    queue_message_id += 1
    timing_results = []
    while True:
        try:
            conn, addr = sock.accept()
        except socket.timeout:
            pass
        except Exception as e:
            raise e
        else:
            print('Connected, beginning RTT measurement')
            data = conn.recv(32)
            for i in range(COMMUNICATION_REPS):
                begin = time.time()
                conn.sendall(b'0')
                data = conn.recv(32)
                end = time.time()
                timing_results.append(end - begin)
            print('Finished RTT measurement')
            break

    df_timing = pd.DataFrame(data=timing_results, columns=["data"])
    df_timing["size"] = 0
    df_timing["queue"] = args.queue
    df_timing["memory"] = memory
    df_timing["type"] = 'rtt'
    dfs.append(df_timing)
    print(f'Begin benchmarking invocations with the queue {args.queue}')
    results = []
    for size in BENCHMARK_SIZES:

        data = generate_binary_data(size)
        print(f"Start repetitions with size {size}")
        for i in range(args.repetitions):
            test_func(data, queue_message_id, addr, port)
            queue_message_id += 1
            begin = time.time()
            data = conn.recv(1024)
            end = time.time()
            ret = json.loads(data.decode())
            results.append([(end - begin), ret['is_cold']])

            if i % 10 == 0:
                print(f"Conducted {i} repetitions out of {args.repetitions}")
        df_invoc = pd.DataFrame(data=results, columns=["data", "is_cold"])
        df_invoc["queue"] = args.queue
        df_invoc["size"] = int(size)
        df_invoc["memory"] = memory
        df_invoc["type"] = 'invocation'
        dfs.append(df_invoc)

    df2 = pd.concat(dfs, axis=0, ignore_index=True)
    df2.to_csv(f"{args.output_prefix}.csv")


