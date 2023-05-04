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

logs_client = boto3.client("logs", region_name='us-east-1')

MEMORY_SIZES = [512, 1024, 2048]
dfs = []
for memory in MEMORY_SIZES:

    data = pd.read_csv(f"{args.input}_{memory}.csv", index_col=[0])
    subset = data.loc[data["memory"] == memory]
    start_timestamp = subset.loc[subset["op"] == "EXPERIMENT_BEGIN"]["data"].values[0]
    end_timestamp = subset.loc[subset["op"] == "EXPERIMENT_END"]["data"].values[0]
    print(start_timestamp)
    print(end_timestamp)

    #requests = set(
    #    subset.loc[
    #        (subset["req"] != "START_TIME") & (subset["request"] != "END_TIME")
    #    ]["request"].values
    #)

    #start_timestamp -= 2*60*60
    #end_timestamp -= 2*60*60
    # now query logs

    for function in ["writer", "distributor"]:
        query = "fields @timestamp, @message | filter @message like /REPORT/ or @message like /Read/ or @message like /RESULT_/"
        log_group = f"/aws/lambda/faaskeeper-test-write-{function}"
        #print(log_group)
        #print(int(start_timestamp))
        #print(int(end_timestamp)+1)
        start_query_response = logs_client.start_query(
            logGroupName=log_group,
            startTime=int(start_timestamp)-40,
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
        print(f"Downloaded: {len(response['results'])}")
        results = []
        res = []
        matched_data = {}
        for result in response["results"]:
            for r in result:
                if r["field"] == "@message":
                    if "REPORT" in r["value"]:
                        res = []
                        for line in r["value"].split("\t"):
                            if line.isspace():
                                continue
                            split = line.split(":")
                            res.append(split[1].split()[0])
                        if len(res) > 5:
                            pass
                            #print(f"Cold invocation! {r['value']}")
                        elif int(res[3]) != int(memory):
                            pass
                            #print(f"Memory {memory}, skip result for {res[3]}")
                        else:
                            #print(f"Found: {res2[0]}")
                            matched_data[res[0]] = res
                    elif "Read:" in r["value"]:
                        res2 = []
                        for line in r["value"].split("\t"):
                            if line.isspace():
                                continue
                            split = line.split(":")
                            res2.append(split[1].split()[0])
                        if res2[0] in matched_data:
                            results.append([*matched_data[res2[0]], *res2])
                        else:
                            pass
                    elif "RESULT_" in r["value"]:
                        print(r["value"])
                        #print(f"Unknonw: {res2[0]}")
                        #print(res)
                        #print(res2)
        #print(results)
        df = pd.DataFrame(
            data=results,
            columns=["request", "time", "billed_time", "memory", "used_memory", "read", "write"],
        )
        df["memory"] = memory
        df["function"] = function
        #print(results)
        dfs.append(df)
        print(
            f"Finished, found: {df.shape[0]} results"
        )
    #            if res[0] not in requests:
    #                #print(f"Skip incorrect request {res[0]}")
    #                pass
    #            elif len(res) > 5:
    #                print(f"Cold invocation! {r['value']}")
    #            else:
    #                cost = subset.loc[(subset["request"] == res[0])]["cost"].values[0]
    #                results.append([cost, *res])
    #                requests.remove(res[0])
    #df["memory"] = memory
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df.to_csv(f"{args.output}_{memory}_processed.csv")

