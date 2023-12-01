#!/bin/bash

dt=$(date '+%Y_%m_%d_%H_%M_%S')
mkdir results_$dt

cnt=1
for server in $@; do

	echo $server $cnt

	ssh ubuntu@$server "killall -SIGINT profiler.x"

	scp ubuntu@$server:zoo.csv results_$dt/zoo_${cnt}.csv

	cnt=$((cnt + 1))

done

./node_stats.py results_$dt/node_stats.csv $1
