import argparse
import base64
import json
import socket
import sys
import time
import urllib
from datetime import datetime, timedelta
from functools import partial

import concurrent
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

import boto3
import pandas as pd

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument('--region', type=str)
parser.add_argument('--output-prefix', type=str)
parser.add_argument('--repetitions', type=int)
parser.add_argument('--workers', type=int)
parser.add_argument('--benchmark', type=str)
parser.add_argument('--size', type=int)
parser.add_argument('--table-name', type=str)
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
    return base64.b64encode(bytearray([1] * original_size))
    #return bytes(bytearray([1]*original_size))

def test_dynamo(dynamo_client, table_name, idx, data):
    data = {
        "data": {'B': data},
        "key": {'S': f"/BENCHMARK_{idx}"}
    }
    dynamo_client.get_item(                                            
        TableName=table_name,
        Key = {'key': {'S': f"/BENCHMARK_{idx}"}}
    )
    dynamo_client.put_item(
        TableName=table_name,
        Item=data,
    )

def run(idx, actual_rps, rps, repetitions, size, bench_type, table_name, output_prefix, workers, barrier):

    dynamo_client = boto3.client('dynamodb', region_name=args.region)
    results = []
    input_data = generate_binary_data(size)
    total_begin = time.time()

    test_func = None
    if bench_type == 'dynamodb':
        test_func = partial(test_dynamo, dynamo_client, table_name)
        data = {
            "data": {'B': input_data},
            "key": {'S': f"/BENCHMARK_{idx}"}
        }
        dynamo_client.put_item(
            TableName=table_name,
            Item=data,
        )
    elif bench_type == 'synch':
        test_func = partial(test_sqs, sqs_queue_url)

    print(f"Worker {idx} doing {rps}")
    period = 1.0/rps
    repetitions = rps * repetitions
    barrier.wait()
    start = time.time()
    for i in range(repetitions):
        #begin = time.time()
        #time.sleep(0.001)
        begin = time.time()
        test_func(idx, input_data)
        end = time.time()
        results.append([begin, end])
        delta = start + period * (i +1) - time.time()
        if delta > 0:
            time.sleep(delta)

    total_end = time.time()#datetime.now()
    results.append([total_begin, -1])
    results.append([total_end, -2])
    df_timing = pd.DataFrame(data=results, columns=["begin", "end"])
    df_timing["benchmatk"] = bench_type
    df_timing["size"] = size
    df_timing["rps"] = rps
    df_timing["worker"] = idx
    df_timing["workers"] = workers
    df_timing["repetitions"] = repetitions

    print(f"Save to {output_prefix}_{actual_rps}_{idx}.csv")
    df_timing.to_csv(f"{output_prefix}_{actual_rps}_{idx}.csv")

rps_arr = list(range(10,130,10))
with ProcessPoolExecutor(max_workers=args.workers) as executor:
    
    mgr = multiprocessing.Manager()
    #barrier = multiprocessing.Barrier(args.workers)
    barrier = mgr.Barrier(args.workers)
    for rps in rps_arr:
        print(f"Start repetitions with rps {rps}")
        futures = []
        for i in range(args.workers-1):
            futures.append(executor.submit(run, i, rps, 120, args.repetitions, args.size, args.benchmark, args.table_name, f"{args.output_prefix}_{args.workers}", args.workers, barrier))
        futures.append(executor.submit(run, args.workers-1, rps, rps, args.repetitions, args.size, args.benchmark, args.table_name, f"{args.output_prefix}_{args.workers}", args.workers, barrier))
        for f in futures:
            f.result()
        #concurrent.futures.wait(futures)


