import argparse
import base64
import json
from datetime import datetime, timedelta

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
parser.add_argument("--max-size", type=int, default=-1)
args = parser.parse_args()

BENCHMARK_SIZES = [2 ** i for i in range(2, 20)]


def generate_binary_data(size):
    # for n bytes the length of base64 string is 4 * n / 3 (unpadded)
    # then it's padded to 4 bytes
    # so the reverse is: n * 3/4 - we always select multiples of fours
    original_size = int(size * 3 / 4)
    #return base64.b64encode(bytearray([1] * original_size))
    return bytes(bytearray([1]*original_size))


cfg = Config.deserialize(json.load(open(args.config)))
service_name = f"faaskeeper-{cfg.deployment_name}"
try:
    client = FaaSKeeperClient(cfg, args.port, False)
    client.start()
    print(f"Connected {client.session_id}")

    dfs = []
    for size in BENCHMARK_SIZES:

        # evaluate at the maximum
        if args.max_size != -1 and size > args.max_size:
            size = args.max_size

        print(f"Execute size {size}")
        data = generate_binary_data(size)
        try:
            client.delete(f"/size_{size}")
        except faaskeeper.exceptions.NodeDoesntExistException:
            pass
        client.create(f"/size_{size}", data)
        client.get_data(f"/size_{size}")
        StorageStatistics.instance().reset()

        results = []
        for i in range(args.repetitions):
            begin = datetime.now()
            node = client.get_data(f"/size_{size}")
            end = datetime.now()
            results.append(int((end - begin) / timedelta(microseconds=1)))
        # sanity check
        if node.data != data:
            raise RuntimeError()

        df_write = pd.DataFrame(data=results, columns=["data"])
        df_write["op"] = "write"
        df_write = df_write.append(
            {"data": StorageStatistics.instance().read_units, "op": "read_capacity"},
            ignore_index=True,
        )
        df_write["size"] = size
        dfs.append(df_write)

        if args.max_size != -1 and size > args.max_size:
            break

    df = pd.concat(dfs, axis=0, ignore_index=True)
    df.to_csv(f"{args.output_prefix}.csv")

    client.stop()
    print("Finished!")
except Exception as e:
    import traceback

    traceback.print_exc()
