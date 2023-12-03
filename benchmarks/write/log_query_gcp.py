import argparse
import json
import time
import math
from datetime import datetime, timedelta, timezone
import boto3
import pandas as pd
import re

from google.cloud import monitoring_v3

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--input", type=str)
# parser.add_argument("--output", type=str)
args = parser.parse_args()

logs_client = boto3.client("logs", region_name='us-east-1')

#######
start_time = 1701116482.875135	 #2023-11-01 02:01:28.999 
end_time = 1701116803.140255
function_name = ["writer", "distributor"]
# function_name = "faaskeeper-dev-distributor"
#######

MEMORY_SIZES = [2048] # 512, 1024, 
dfs = []
for memory in MEMORY_SIZES:

    from google.api_core import exceptions
    from time import sleep

    def wrapper(gen):
        while True:
            try:
                yield next(gen)
            except StopIteration:
                break
            except exceptions.ResourceExhausted:
                print("Google Cloud resources exhausted, sleeping 30s")
                sleep(30)

    """
        Use GCP's logging system to find execution time of each function invocation.

        There shouldn't be problem of waiting for complete results,
        since logs appear very quickly here.
    """
    from google.cloud import logging as gcp_logging

    logging_client = gcp_logging.Client()
    logger = logging_client.logger("cloudfunctions.googleapis.com%2Factivity")#"cloudfunctions.googleapis.com%2Factivity"

    """
        GCP accepts only single date format: 'YYYY-MM-DDTHH:MM:SSZ'.
        Thus, we first convert timestamp to UTC timezone.
        Then, we generate correct format.

        Add 1 second to end time to ensure that removing
        milliseconds doesn't affect query.
    """
    timestamps = []
    for timestamp in [start_time, end_time]:
        utc_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        timestamps.append(utc_date.strftime("%Y-%m-%dT%H:%M:%SZ"))

    print(timestamps)
    invocations = logger.list_entries(
        filter_=(
            f'resource.labels.function_name = "{function_name}" '
            f'timestamp >= "{timestamps[0]}" '
            f'timestamp <= "{timestamps[1]}"'
        ),
        page_size=1000,
    )
    invocations_processed = 0
    if hasattr(invocations, "pages"):
        pages = list(wrapper(invocations.pages))
    else:
        pages = [list(wrapper(invocations))]

    entries = 0
    for page in pages:  # invocations.pages:
        print(page)
        for invoc in page:
            entries += 1
            if "execution took" in invoc.payload:
                execution_id = invoc.labels["execution_id"]
                # might happen that we get invocation from another experiment
                # if execution_id not in requests:
                #     continue
                # find number of miliseconds
                regex_result = re.search(r"\d+ ms", invoc.payload)
                assert regex_result
                exec_time = regex_result.group().split()[0]
                # convert into microseconds
                # requests[execution_id].provider_times.execution = int(exec_time) * 1000
                invocations_processed += 1
    print(
        f"GCP: Received {entries} entries, found time metrics for {invocations_processed} "
        # f"out of {len(requests.keys())} invocations."
    )

    """
        Use metrics to find estimated values for maximum memory used, active instances
        and network traffic.
        https://cloud.google.com/monitoring/api/metrics_gcp#gcp-cloudfunctions
    """

    # Set expected metrics here
    available_metrics = ["execution_times", "user_memory_bytes", "network_egress"]

    client = monitoring_v3.MetricServiceClient()
    project_name = client.common_project_path("wide-axiom-402003")

    end_time_nanos, end_time_seconds = math.modf(end_time)
    start_time_nanos, start_time_seconds = math.modf(start_time)

    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": int(end_time_seconds) + 60},
            "start_time": {"seconds": int(start_time_seconds)},
        }
    )
    metrics = {}
    dfs = []
    
    for metric in available_metrics:

        metrics[metric] = [0,0]

        list_request = monitoring_v3.ListTimeSeriesRequest(
            name=project_name,
            filter='metric.type = "cloudfunctions.googleapis.com/function/{}"'.format(metric),
            interval=interval,
        )
        for function in function_name:
            results = client.list_time_series(list_request)
            vals = []
            repts = []
            for result in results:
                if result.resource.labels.get("function_name") == "faaskeeper-dev-" + function:
                    for point in result.points:
                        # metrics[metric][0] += point.value.distribution_value.mean * point.value.distribution_value.count
                        # metrics[metric][1] += point.value.distribution_value.count
                        vals.append(point.value.distribution_value.mean)
                        repts.append(point.value.distribution_value.count)
            df_invoc = pd.DataFrame(data=vals, columns=["mean_value"])
            df_invoc["execution_counts"] = repts
            df_invoc["type"] = metric
            df_invoc["function"] = function
            dfs.append(df_invoc)
    df2 = pd.concat(dfs, axis=0, ignore_index=True)
    df2.to_csv(f"persistent_4_512_1v_processed.csv")
    # df = pd.concat(dfs, axis=0, ignore_index=True)
    # df.to_csv(f"{args.output}_{memory}_processed.csv")

