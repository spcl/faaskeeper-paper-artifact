#!/bin/bash

PORT=60000
REPETITIONS=100
CFG=test_heartbeat.json

for clients in 1 2 4 8 16 32 64; do

  pids=()
  for (( i=0; i<$clients; i++ )); do
    p=$((PORT + i))
    echo "Start client $i at port ${p}"
    python3 test.py --config $CFG --port $p &
    PID=$!
    echo "Start $PID"
    pids+=($PID)
  done

  # do computation
  python3 benchmark.py --clients $clients --repetitions $REPETITIONS --output-prefix benchmark_output_$clients

  for value in "${pids[@]}"; do
    echo "Kill $value"
    kill $value
  done

  for value in "${pids[@]}"; do
    while kill -0 $value; do 
      sleep 1
    done
  done


done
