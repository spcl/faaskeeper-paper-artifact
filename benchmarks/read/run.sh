#!/bin/bash

PORT=60000
REPETITIONS=1000

python3 benchmark.py --config test_read.json --port $PORT --repetitions $REPETITIONS --max-size 408576 --output-prefix ../../data/read/key_value
# FIXME: reconfigure functions
#python3 benchmark.py --config test_read.json --port $PORT --repetitions $REPETITIONS --output-prefix ../../data/read/persistent
