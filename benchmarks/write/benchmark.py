import argparse
import base64
import json
import time
from datetime import datetime, timedelta

import boto3
import pandas as pd

import faaskeeper
from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import CloudProvider, Config
from faaskeeper.stats import StorageStatistics

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--port", type=int)
parser.add_argument("--config", type=str)
parser.add_argument("--output-prefix", type=str)
parser.add_argument("--repetitions", type=int)
parser.add_argument("--size", type=int)
args = parser.parse_args()

# BENCHMARK_SIZES = [4]  # [2 ** i for i in range(2, 20)]
size = args.size
#MEMORY = [128,256,512,1024,2048]
#MEMORY = [512,1024,2048]
MEMORY = [2048]
lambda_client = boto3.client("lambda", region_name="us-east-1")


def generate_binary_data(size):
    # for n bytes the length of base64 string is 4 * n / 3 (unpadded)
    # then it's padded to 4 bytes
    # so the reverse is: n * 3/4 - we always select multiples of fours
    original_size = int(size * 3 / 4)
    #return base64.b64encode(bytearray([1] * original_size))
    return bytearray([1] * original_size)
    #return bytes(bytearray([1] * original_size))


cfg = Config.deserialize(json.load(open(args.config)))
service_name = f"faaskeeper-{cfg.deployment_name}"
fname1 = "faaskeeper-test-write-writer"
fname2 = "faaskeeper-test-write-distributor"
try:
    for memory in MEMORY:
        print(f"Update config to {memory}")
        lambda_client.update_function_configuration(
            FunctionName=fname1, Timeout=30, MemorySize=memory
        )
        lambda_client.update_function_configuration(
            FunctionName=fname2, Timeout=30, MemorySize=memory
        )
        client = FaaSKeeperClient(cfg, args.port, False)
        client.start()
        print(f"Connected {client.session_id}")

        dfs = []
        print(f"Execute size {size}")
        data = generate_binary_data(size)
        try:
            client.delete(f"/size_{size}")
        except faaskeeper.exceptions.NodeDoesntExistException:
            pass
        client.create(f"/size_{size}", data)
        client.get_data(f"/size_{size}")

        time.sleep(10)
        print("Done")
        experiment_begin = datetime.now().timestamp()
        time.sleep(20)
        print("Begin")
        print(f"Execute memory {memory}")

        # evaluate at the maximum

        StorageStatistics.instance().reset()

        results = []
        for i in range(args.repetitions):
            begin = datetime.now()
            try:
                node = client.set_data(f"/size_{size}", data, -1)
            except faaskeeper.exceptions.TimeoutException:
                print(f"FAILURE ON {i}")
                continue
            end = datetime.now()
            results.append(int((end - begin) / timedelta(microseconds=1)))
            if i % 10 == 0:
                print(f"Repetition {i}")
        # sanity check
        experiment_end = datetime.now().timestamp()
        # if node.data != data:
        #    raise RuntimeError()

        df_write = pd.DataFrame(data=results, columns=["data"])
        df_write["client_write_data"] = StorageStatistics.instance().write_times
        df_write["op"] = "set_data"
        df_write = df_write.append(
            {
                "data": StorageStatistics.instance().write_units,
                "op": "client_write_capacity",
            },
            ignore_index=True,
        )
        df_write = df_write.append(
            {"data": experiment_begin, "op": "EXPERIMENT_BEGIN"}, ignore_index=True,
        )
        df_write = df_write.append(
            {"data": experiment_end, "op": "EXPERIMENT_END"}, ignore_index=True,
        )
        df_write["memory"] = memory
        df_write["size"] = size
        #dfs.append(df_write)
        df_write.to_csv(f"{args.output_prefix}_{memory}.csv")
        client.stop()

    #df = pd.concat(dfs, axis=0, ignore_index=True)
    #df.to_csv(f"{args.output_prefix}.csv")

    print("Finished!")
except Exception as e:
    import traceback

    traceback.print_exc()
