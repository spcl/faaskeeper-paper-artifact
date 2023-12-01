from google.cloud import logging as gcp_logging
import pandas as pd

logging_client = gcp_logging.Client()
#logger = logging_client.logger("cloudfunctions.googleapis.com%2Fcloud-functions")
logger = logging_client.logger("run.googleapis.com%2Frequests")

timestamps = []
#for timestamp in [start_time, end_time + 1]:
#utc_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
#timestamps.append(utc_date.strftime("%Y-%m-%dT%H:%M:%SZ"))


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

from datetime import datetime, timedelta, timezone
start_time = 1701120201.746827
end_time = 1701120537.629221
memory = 512

timestamps = []
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
            results.append([m, memory,f_n])
            entries += 1

df = pd.DataFrame(
    data=results,
    columns=["billed_time", "memory", "function"],
)

df.to_csv(f"persistent_262144_1vCPU_512_processed.csv")
#for page in pages:  # invocations.pages:
#for invoc in page:
#    entries += 1
#    if "execution took" in invoc.payload:
#        execution_id = invoc.labels["execution_id"]
#        # might happen that we get invocation from another experiment
#        if execution_id not in requests:
#            continue
#        # find number of miliseconds
#        regex_result = re.search(r"\d+ ms", invoc.payload)
#        assert regex_result
#        exec_time = regex_result.group().split()[0]
#        # convert into microseconds
#        requests[execution_id].provider_times.execution = int(exec_time) * 1000
#        invocations_processed += 1
#self.logging.info(
#f"GCP: Received {entries} entries, found time metrics for {invocations_processed} "
#f"out of {len(requests.keys())} invocations."
#)
