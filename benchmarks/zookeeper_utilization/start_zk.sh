#!/bin/bash

cnt=1
for server in $@; do

	echo $server $cnt

	scp config/zoo${cnt}.cfg ubuntu@$server:zoo.cfg

	ssh ubuntu@$server "mkdir -p /home/ubuntu/data/ && echo $cnt > /home/ubuntu/data/myid && /home/ubuntu/zookeeper/bin/zkServer.sh --config . start"

	#ssh ubuntu@$server "nohup /home/ubuntu/profiler.x 50 ens5 zoo.csv 2181 > profiler.log 2>&1 &"

	cnt=$((cnt + 1))

done
