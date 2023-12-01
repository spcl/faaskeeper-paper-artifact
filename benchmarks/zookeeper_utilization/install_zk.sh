#!/bin/bash

cnt=1
for server in $@; do

	echo $server

	scp src/profiler.cpp ubuntu@$server:.

	ssh ubuntu@$server "sudo apt-get update &&\
    sudo apt-get install -y openjdk-11-jre g++ &&\
    wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz &&\
    tar -xf apache-zookeeper-3.7.2-bin.tar.gz &&\
    mv apache-zookeeper-3.7.2-bin zookeeper &&\
    g++ profiler.cpp -o profiler.x"

done
