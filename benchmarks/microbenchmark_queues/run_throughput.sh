#!/bin/bash

duration=10
QUEUE=$1
NAME=$2
id=$3
for rps in 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 105 110 115 120 125 130 135 140 145 150 155 160 165 170 175 180 185 190 195 200; do
	repetitions=$(( rps*duration))
	echo "Begin for rps $rps, duration $duration, repetitions $repetitions, message id beginning $id"
	time python3 receiver.py --region us-east-1 --output-prefix data/receiver_${rps} --repetitions ${repetitions} --queue $QUEUE --queue-name ${NAME} --port 60000 --fname faaskeeper-microbenchmark-queue-dev-benchmarker_multiple --message-id $id --memory 128 --size 1024 > data/receiver_${rps}.out &
	pid=$!
	python3 throughput_generator.py --region us-east-1 --output-prefix data/generator_${rps} --repetitions $duration --rps $rps --queue ${QUEUE} --queue-name $NAME --port 60000 --fname faaskeeper-microbenchmark-queue-dev-benchmarker_multiple --message-id $id --memory 128 --size 64 > data/throughput_${rps}.out
	id=$((id + repetitions + 50))
	wait $pid
done
