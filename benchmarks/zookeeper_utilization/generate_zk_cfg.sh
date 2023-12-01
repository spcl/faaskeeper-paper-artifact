#!/bin/bash

ZK1=$1
ZK2=$2
ZK3=$3

cnt=1
mkdir -p config
head -n 6 ../zookeeper_deployment/zoo${cnt}.cfg >config/zoo${cnt}.cfg
echo "4lw.commands.whitelist=mntr,stat,srvr,cons,crst" >>config/zoo${cnt}.cfg
echo "server.1=0.0.0.0:2888:3888" >>config/zoo${cnt}.cfg
echo "server.2=${ZK2}:2888:3888" >>config/zoo${cnt}.cfg
echo "server.3=${ZK3}:2888:3888" >>config/zoo${cnt}.cfg

cnt=2
mkdir -p config
head -n 6 ../zookeeper_deployment/zoo${cnt}.cfg >config/zoo${cnt}.cfg
echo "4lw.commands.whitelist=mntr,stat,srvr,cons,crst" >>config/zoo${cnt}.cfg
echo "server.1=${ZK1}:2888:3888" >>config/zoo${cnt}.cfg
echo "server.2=0.0.0.0:2888:3888" >>config/zoo${cnt}.cfg
echo "server.3=${ZK3}:2888:3888" >>config/zoo${cnt}.cfg

cnt=3
mkdir -p config
head -n 6 ../zookeeper_deployment/zoo${cnt}.cfg >config/zoo${cnt}.cfg
echo "4lw.commands.whitelist=mntr,stat,srvr,cons,crst" >>config/zoo${cnt}.cfg
echo "server.1=${ZK1}:2888:3888" >>config/zoo${cnt}.cfg
echo "server.2=${ZK2}:2888:3888" >>config/zoo${cnt}.cfg
echo "server.3=0.0.0.0:2888:3888" >>config/zoo${cnt}.cfg
