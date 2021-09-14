import argparse
import json
import signal
import time

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import CloudProvider, Config

parser = argparse.ArgumentParser(description="Run microbenchmarks.")
parser.add_argument("--port", type=int)
parser.add_argument("--config", type=str)
args = parser.parse_args()


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True


cfg = Config.deserialize(json.load(open(args.config)))
service_name = f"faaskeeper-{cfg.deployment_name}"
try:
    client = FaaSKeeperClient(cfg, args.port, False)
    client.start()
    print(f"Connected {client.session_id}")
    # wait for kill
    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(1)
    client.stop()
    print("Finished!")
except Exception as e:
    import traceback

    traceback.print_exc()
