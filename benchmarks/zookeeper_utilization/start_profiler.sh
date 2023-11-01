#!/bin/bash

cnt=1
for server in $@; do

	echo $server $cnt

	ssh ubuntu@$server "nohup /home/ubuntu/profiler.x 50 ens5 zoo.csv 2181 > profiler.log 2>&1 &"

	cnt=$((cnt + 1))

done
