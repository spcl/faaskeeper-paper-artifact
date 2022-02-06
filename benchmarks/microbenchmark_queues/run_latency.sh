#!/bin/bash

id=1000
repetitions=100
for m in 2048; do
	echo $id
	python3 benchmark_multiple.py --region us-east-1 --output-prefix sqs_fifo_${m} --repetitions ${repetitions} --queue sqs_fifo --queue-name BENCHMARK_SQS_QUEUE_LATENCY.fifo --port 60000 --fname faaskeeper-microbenchmark-queue-dev-benchmarker --message-id $id --memory $m
	id=$((id + repetitions*5))
done
