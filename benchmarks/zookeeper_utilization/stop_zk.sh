#!/bin/bash

cnt=1
for server in $@; do

	echo $server
	ssh ubuntu@$server "/home/ubuntu/zookeeper/bin/zkServer.sh --config . stop"

done
