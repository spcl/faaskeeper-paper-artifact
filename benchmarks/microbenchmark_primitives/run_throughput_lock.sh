#!/bin/bash

#for processes in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16; do
for processes in 1 2 3 4 5 6 7 8 9 10; do
	echo $processes
	python3 throughput_generator.py --region us-east-1 --output-prefix lock --repetitions 5 --table-name BENCHMARK_DYNAMO_PRIMITIVES  --workers $processes --benchmark lock --size 64
done
