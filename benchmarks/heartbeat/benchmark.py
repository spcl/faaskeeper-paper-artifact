import argparse
import json
import time
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--output-prefix", type=str)
parser.add_argument("--repetitions", type=int)
parser.add_argument("--clients", type=int)
args = parser.parse_args()

# in bytes
# base64 encoded string begins at 4 bytes anyway

MEMORY_SIZES = [128, 256]

print(f"Initialize")
lambda_client = boto3.client("lambda")
logs_client = boto3.client("logs")

dfs = []
costs = []
requests = set()

clients = args.clients

fname = "faaskeeper-dev-heartbeat"
dfs = []
for memory in MEMORY_SIZES:

    print(f"Update config to {memory}")
    lambda_client.update_function_configuration(
        FunctionName=fname, Timeout=30, MemorySize=memory
    )
    print("Done")
    time.sleep(10)
    print("Begin")
    res = lambda_client.invoke(FunctionName=fname)["Payload"].read()
    # time.sleep(5)
    start = datetime.now(tz=timezone.utc)
    results = []
    results.append(["START_TIME", (start - timedelta(seconds=30)).timestamp()])
    for i in range(args.repetitions + 10):

        invoc = lambda_client.invoke(FunctionName="faaskeeper-dev-heartbeat")
        res = json.loads(invoc["Payload"].read().decode())
        clients = res["active_clients"]
        if clients != args.clients:
            raise RuntimeError(f"Repetition {i}, clients {clients}!")
        # costs.append(res["cost"])
        # requests.add(invoc["ResponseMetadata"]["RequestId"])
        results.append([invoc["ResponseMetadata"]["RequestId"], res["cost"]])

    end = datetime.now(tz=timezone.utc)
    results.append(["END_TIME", (end + timedelta(seconds=30)).timestamp()])
    df = pd.DataFrame(data=results, columns=["request", "cost"])
    df["memory"] = memory
    dfs.append(df)

# print(
#    f"Finished {args.repetitions+10}, repetitions, didn't find results for: {requests}"
# )
df = pd.concat(dfs, axis=0, ignore_index=True)
df["clients"] = clients
df.to_csv(f"{args.output_prefix}.csv")

