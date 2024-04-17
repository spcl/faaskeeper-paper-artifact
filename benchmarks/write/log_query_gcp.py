import argparse
import json
import time
import math
from datetime import datetime, timedelta, timezone
import pandas as pd

from google.cloud import monitoring_v3
from google.cloud import logging as gcp_logging
"""
    Use GCP's logging system to find execution time of each function invocation.

    There shouldn't be problem of waiting for complete results,
    since logs appear very quickly here.
"""
start_time = 1700977949.064928
end_time = 1700978284.283415
memory = 512

logging_client = gcp_logging.Client()
#logger = logging_client.logger("cloudfunctions.googleapis.com%2Fcloud-functions")
logger = logging_client.logger("run.googleapis.com%2Frequests")

timestamps = []
#for timestamp in [start_time, end_time + 1]:
#utc_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
#timestamps.append(utc_date.strftime("%Y-%m-%dT%H:%M:%SZ")

from google.api_core import exceptions
from time import sleep
def wrapper(gen):
    while True:
        try:
            yield next(gen)
        except StopIteration:
            break
        except exceptions.ResourceExhausted:
            self.logging.info("Google Cloud resources exhausted, sleeping 30s")
            sleep(30)

for timestamp in [start_time, end_time + 1]:
    utc_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    timestamps.append(utc_date.strftime("%Y-%m-%dT%H:%M:%SZ"))

tt = "faaskeeper-dev-"
function_names=['writer', 'distributor']
# timestamps=['2023-12-01T21:41:00', '2023-12-01T21:45:19']
results = []

for f_n in function_names:
    function_name = tt + f_n
    invocations = logger.list_entries(
        filter_=(f'''
            (resource.type="cloud_run_revision" resource.labels.service_name="{function_name}")
            OR
            (resource.type="cloud_function" resource.labels.function_name="{function_name}")
            severity>=INFO
            timestamp >= "{timestamps[0]}"
            timestamp <= "{timestamps[1]}"
        '''
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
        for invoc in page:

            # convert to nanoseconds
            m = float(invoc.http_request['latency'][0:-1]) * 1000 * 1000 * 1000
            # convert to milliseconds
            rm = math.ceil(m * 0.00000001)
            # print(m * 0.000001 ,rm * 100)
            # break
            results.append([ m * 0.000001 ,rm * 100, memory,f_n])
            entries += 1

df = pd.DataFrame(
    data=results,
    columns=["time","billed_time", "memory", "function"],
)

df.to_csv(f"persistent_262144_512_processed.csv")

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