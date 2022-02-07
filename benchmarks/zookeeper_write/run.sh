#!/bin/bash

PORT=60000
REPETITIONS=150
ADDR=18.206.123.7

for size in 4 1024 65536 131072 256000; do
	script -e -c "python3 benchmark.py --addr ${ADDR} --repetitions $REPETITIONS --output-prefix ../../data/write_zookeeper/interregion_${size} --size $size" -f ../../data/write_zookeeper/interregion_${size}.log
done
# FIXME: reconfigure functions
#python3 benchmark.py --config test_read.json --port $PORT --repetitions $REPETITIONS --output-prefix ../../data/read/persistent
