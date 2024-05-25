#!/bin/bash

cnt=1
for server in $@; do

	echo $server
	ssh ubuntu@$server "rm -rf /home/ubuntu/data/version-2"

done
