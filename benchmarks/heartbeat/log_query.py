import argparse
import json
import time
from datetime import datetime, timedelta

import boto3
import pandas as pd

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--input", type=str)
parser.add_argument("--output", type=str)
args = parser.parse_args()

data = pd.read_csv(args.input, index_col=[0])
logs_client = boto3.client("logs", region_name='us-east-1')

MEMORY_SIZES = [128, 256, 512, 1024, 1536, 2048]
dfs = []
for memory in MEMORY_SIZES:

    subset = data.loc[data["memory"] == memory]
    start_timestamp = subset.loc[subset["request"] == "START_TIME"]["cost"].values[0]
    end_timestamp = subset.loc[subset["request"] == "END_TIME"]["cost"].values[0]
    print(start_timestamp)

    requests = set(
        subset.loc[
            (subset["request"] != "START_TIME") & (subset["request"] != "END_TIME")
        ]["request"].values
    )

    #start_timestamp -= 2*60*60
    #end_timestamp -= 2*60*60
    # now query logs
    query = "fields @timestamp, @message | filter @message like /REPORT/"
    log_group = "/aws/lambda/faaskeeper-test-heartbeat-heartbeat"
    #print(int(start_timestamp))
    #print(int(end_timestamp)+1)
    start_query_response = logs_client.start_query(
        logGroupName=log_group,
        startTime=int(start_timestamp)-60,
        endTime=int(end_timestamp)+1+60,
        queryString=query,
        limit=10000
    )
    query_id = start_query_response["queryId"]
    response = None
    while response is None or response["status"] == "Running":
        print("Waiting for query to complete ...")
        time.sleep(1)
        response = logs_client.get_query_results(queryId=query_id)

    # print(response)
    results = []
    #print(response)
    for result in response["results"]:
        for r in result:
            if r["field"] == "@message":
                res = []
                for line in r["value"].split("\t"):
                    if line.isspace():
                        continue
                    split = line.split(":")
                    res.append(split[1].split()[0])
                if res[0] not in requests:
                    #print(f"Skip incorrect request {res[0]}")
                    pass
                elif len(res) > 5:
                    print(f"Cold invocation! {r['value']}")
                else:
                    cost = subset.loc[(subset["request"] == res[0])]["cost"].values[0]
                    results.append([cost, *res])
                    requests.remove(res[0])
    df = pd.DataFrame(
        data=results,
        columns=["cost", "request", "time", "billed_time", "memory", "used_memory"],
    )
    df["memory"] = memory
    dfs.append(df)
    print(
        f"Finished, didn't find results for: {requests}"
    )
df = pd.concat(dfs, axis=0, ignore_index=True)
df.to_csv(f"{args.output}.csv")
