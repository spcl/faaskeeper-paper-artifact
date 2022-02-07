import argparse
import base64
import json
from datetime import datetime, timedelta

import pandas as pd

from kazoo.client import KazooClient

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--addr", type=str)
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
    return base64.b64encode(bytearray([1] * original_size))
    #return bytes(bytearray([1]*original_size))


try:

    client = KazooClient(hosts=f'{args.addr}:2181')
    client.start()

    dfs = []
    for size in BENCHMARK_SIZES:

        # evaluate at the maximum
        if args.max_size != -1 and size > args.max_size:
            size = args.max_size

        print(f"Execute size {size}")
        data = generate_binary_data(size)
        try:
            client.delete(f"/size_{size}")
        except Exception as e:
            print(e)
        client.create(f"/size_{size}", data)
        client.get(f"/size_{size}")

        results = []
        for i in range(args.repetitions):
            begin = datetime.now()
            node = client.get(f"/size_{size}")
            end = datetime.now()
            results.append(int((end - begin) / timedelta(microseconds=1)))
        # sanity check
        if node[0] != data:
            raise RuntimeError()

        df_write = pd.DataFrame(data=results, columns=["data"])
        df_write['native_data'] = 0
        df_write["op"] = "read"
        df_write = df_write.append(
            {"data": 0, "op": "read_capacity"},
            ignore_index=True,
        )
        df_write["size"] = size
        dfs.append(df_write)

        if args.max_size != -1 and size > args.max_size:
            break

    df = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"Save result to {args.output_prefix}.csv")
    df.to_csv(f"{args.output_prefix}.csv")

    client.stop()
    print("Finished!")
except Exception as e:
    import traceback

    traceback.print_exc()
