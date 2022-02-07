import argparse
import base64
import json
import time
from datetime import datetime, timedelta

import boto3
import pandas as pd

from kazoo.client import KazooClient

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--addr", type=str)
parser.add_argument("--output-prefix", type=str)
parser.add_argument("--repetitions", type=int)
parser.add_argument("--size", type=int)
args = parser.parse_args()

# BENCHMARK_SIZES = [4]  # [2 ** i for i in range(2, 20)]
size = args.size
#MEMORY = [128,256,512,1024,2048]
MEMORY = [512,1024,2048]
lambda_client = boto3.client("lambda", region_name="us-east-1")


def generate_binary_data(size):
    # for n bytes the length of base64 string is 4 * n / 3 (unpadded)
    # then it's padded to 4 bytes
    # so the reverse is: n * 3/4 - we always select multiples of fours
    original_size = int(size * 3 / 4)
    return base64.b64encode(bytearray([1] * original_size))
    #return bytes(bytearray([1] * original_size))

try:
    print(f'{args.addr}:2181')
    client = KazooClient(hosts=f'{args.addr}:2181')
    client.start()

    dfs = []
    print(f"Execute size {args.size}")
    data = generate_binary_data(args.size)
    try:
        client.delete(f"/size_{size}")
    except Exception as e:
        print(e)
    client.create(f"/size_{size}", data)
    client.get(f"/size_{size}")


    results = []
    for i in range(args.repetitions):
        begin = datetime.now()
        node = client.set(f"/size_{size}", data)
        end = datetime.now()
        results.append(int((end - begin) / timedelta(microseconds=1)))
        if i % 10 == 0:
            print(f"Repetition {i}")
    # sanity check
    experiment_end = datetime.now().timestamp()
    node = client.get(f"/size_{size}")
    if node[0] != data:
        raise RuntimeError()
    # if node.data != data:
    #    raise RuntimeError()

    df_write = pd.DataFrame(data=results, columns=["data"])
    df_write["op"] = "set_data"
    df_write["size"] = size
    #dfs.append(df_write)
    df_write.to_csv(f"{args.output_prefix}.csv")
    client.stop()

    #df = pd.concat(dfs, axis=0, ignore_index=True)
    #df.to_csv(f"{args.output_prefix}.csv")

    print("Finished!")
except Exception as e:
    import traceback

    traceback.print_exc()
