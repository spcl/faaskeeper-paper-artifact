#!/bin/bash

duration=10
id=100
for rps in 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 105 110 115 120 125 130 135 140 145 150 155 160 165 170 175 180 185 190 195 200; do
	repetitions=$(( rps*duration))
	echo "Begin for rps $rps, duration $duration, repetitions $repetitions, message id beginning $id"
	python3 receiver.py --region us-east-1 --output-prefix data/receiver_${rps} --repetitions ${repetitions} --queue sqs_fifo --queue-name BENCHMARK_SQS_QUEUE.fifo --port 60000 --fname faaskeeper-microbenchmark-queue-dev-benchmarker_multiple --message-id $id --memory 128 --size 1024 > data/receiver_${rps}.out &
	pid=$!
	python3 throughput_generator.py --region us-east-1 --output-prefix data/generator_${rps} --repetitions $duration --rps $rps --queue sqs_fifo --queue-name BENCHMARK_SQS_QUEUE_MULTIPLE.fifo --port 60000 --fname faaskeeper-microbenchmark-queue-dev-benchmarker_multiple --message-id $id --memory 128 --size 64 > data/throughput_${rps}.out
	id=$((id + repetitions + 50))
	wait $pid
done
