
We follow this tutorial: https://sameer.page/Distributed-Hadoop-Cluster-HBase-Amazon-EC2

## Preparation

* To set up Hadoop and HBase, we need four VMs - one master and three slaves.
* Make sure you can SSH from master to slave VMs - edit `.ssh/config` such that `ssh slave1` works.
* Set up three slaves, and a master on slave1:
  ```
  sudo hostnamectl set-hostname --static master
  for slave in slave1 slave2 slave3; do ssh ${slave} sudo hostnamectl set-hostname --static ${slave}; done 
  ```
* Create `/etc/cloud/cloud.cfg` on master with `preserve_hostname=true`. Edit `/etc/hosts` with IP addresses of each machine associated with the hostname, as in the example below.
	```
	3.235.193.146 master
	44.197.234.145 slave1
	35.173.57.220 slave2
	35.175.119.14 slave3
	```

* Copy files to other machines:
	```
  # copy data to all slaves
  scp /etc/cloud/cloud.cfg slave2:/tmp && ssh slave2 "sudo mv /tmp/cloud.cfg /etc/cloud/cloud.cfg"
  for slave in slave1 slave2 slave3; do scp /etc/hosts ${slave}:/tmp; ssh ${slave} sudo mv /tmp/hosts /etc/hosts; done
  ```
* In `/etc/hosts`, master needs its private IP - otherwise it won't be able to bind to the port.

## Hadoop

* Install Hadoop in the installation directory and configure Hadoop. The easiest way is to create a `.zshrc` file with this config and copy to other machines `for slave in slave1 slave2 slave3; do scp ~/.zshrc ${slave}:.; done`.
  ```
  export JAVA_HOME=/usr/lib/jvm/java-18-openjdk-amd64
  # Hadoop
  export HADOOP_INSTALL=/home/ubuntu/hbase_benchmarks/hadoop
  export PATH=$PATH:$HADOOP_INSTALL/bin
  export PATH=$PATH:$HADOOP_INSTALL/sbin
  export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
  export HADOOP_COMMON_HOME=$HADOOP_INSTALL
  export HADOOP_HDFS_HOME=$HADOOP_INSTALL
  export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
  export HADOOP_OPTS="-Djava.library.path=$HADOOP_INSTALL/lib"
  ```
* Then, we apply the long list of configuration file changes - all are available in the tutorial above. We create files locally and later copy all of them to `HADOOP_INSTALL/etc/hadoop`.
	- `core-site.xml`  
	```
	<?xml version="1.0" encoding="UTF-8"?>  
	<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>  
	<configuration>
	    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
	</property>
	</configuration>  
	```  
	- In `hadoop-env.sh`, add `JAVA_HOME=/usr/lib/jvm/java-18-openjdk-amd64/`
	- In `hdfs-site.xml`, add  
	```  
	<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
	</property>
	<property>
      <name>dfs.namenode.name.dir</name>
      <value>file:/usr/local/hadoop_store/hdfs/namenode</value>
	</property>
	  <property>
      <name>dfs.datanode.data.dir</name>
      <value>file:/usr/local/hadoop_store/hdfs/datanode</value>
  	</property>
	</configuration>
  
	```	
* On master, we create the directories for HDFS store and copy them to other nodes as well.
  ```
  sudo mkdir -p /usr/local/hadoop_store/hdfs/namenode
  sudo mkdir -p /usr/local/hadoop_store/hdfs/datanode
  sudo chown -R ubuntu:ubuntu /usr/local/hadoop_store/
  for slave in slave1 slave2 slave3; do ssh ${slave} "sudo mkdir -p /usr/local/hadoop_store/hdfs/namenode && sudo mkdir -p /usr/local/hadoop_store/hdfs/datanode && sudo chown -R ubuntu:ubuntu /usr/local/hadoop_store/"; done
  ```
* Create file `~/hbase_benchmarks/etc/hadoop/workers` with list of slaves, and `masters` with the single line `master`.
	```
  slave1
  slave2
  slave3
	```
* From master, we copy the newly created configuration to each slave.
  ```
  cp config/hadoop/* hadoop/etc/hadoop/
  for slave in slave1 slave2 slave3; do scp -r config ${slave}:/home/ubuntu/hbase_benchmarks; done
  for slave in slave1 slave2 slave3; do ssh ${slave} cp hbase_benchmarks/config/hadoop/\* hbase_benchmarks/hadoop/etc/hadoop; done
  ```
* On Master, run `hadoop/bin/hdfs namenode -format`. Should finish after few minutes.
* Make sure port 9000 is open. Also ports 9866, 9867, and 9864.
* Run `hadoop/sbin/start-dfs.sh` on master. `hadoop/bin/hdfs dfsadmin -report` should show three datanodes. Running `jps` should show two proceses on master - `NameNode` and `SecondaryNameNode`. On slave nodes, `jps` should return `DataNode`.

## HBase

- Create the new configuration that will later be copied to `hbase/conf`
	- Inside `hbase-env.sh`, add line `export JAVA_HOME=/usr/lib/jvm/java-18-openjdk-amd64/`.
	- Create `hbase-site.xml` - this is very important as we need to change ZK quorum by adding the servers we created on AWS.  
		```
		<configuration>
		
  			<property>
    			<name>hbase.cluster.distributed</name>
    			<value>true</value>
  			</property>
  			
  			<property>
  				<name>hbase.rootdir</name>
  				<value>hdfs://master:9000/hbase</value>
			</property>
			
			<!-- https://stackoverflow.com/questions/70523635/hbase-shell-org-apache-hadoop-hbase-ipc-servernotrunningyetexception-server-i -->
			<property>
				<name>hbase.wal.provider</name>
				<value>filesystem</value>
			</property>

			<property>
				<name>hbase.zookeeper.property.clientPort</name>
				<value>2181</value>
			</property>
			
			<property>
		    		<name>hbase.zookeeper.quorum</name>
		    		<value>HERE COMES YOUR ZK SERVERS - comma seperated list of addresses</value>
		    	</property>
		</configuration>
		```
- Add `HBASE_HOME` to `.zshrc` and run `for slave in slave1 slave2 slave3; do scp ~/.zshrc ${slave}:.; done`.
- In `hbase/confg/regionservers`, add list of slaves.
- On each slave, its entry in `/etc/hosts` must be the private IP.
- We run the following command to prepare HBase`for slave in slave1 slave2 slave3; do ssh ${slave} "sudo mkdir -p /usr/local/hbase && sudo chown -R ubuntu:ubuntu /usr/local/hbase"; done`
- Now copy the configuration:  
	```  
	for slave in slave1 slave2 slave3; do scp -r config ${slave}:/home/ubuntu/hbase_benchmarks; done`
	for slave in slave1 slave2 slave3; do ssh ${slave} "cp hbase_benchmarks/config/hbase/* hbase_benchmarks/hbase/conf"; done
	```
- Open ports on VMs: 60000, 60010, 60020, 60030
- Run `hbase/bin/start-hbase.sh` on master. If you check active processes with `jps`, on master you should find `HMaster` and `ResourceManager`, and each slave should have `HRegionServer`.

## Benchmark

- Launch new benchmarking instance and install there `ycsb` in version `0.17`.
- Start with pre-splitting as described in README - run this inside `hbase shell`.
  ```
  n_split = 30
  create 'usertable', 'family', {SPLITS => (1..n_splits).map {|i| "user#{1000+i*(9999-1000)/n_splits}"}}
  ```
- Copy IP addresses of master and slaves to `/etc/hosts` on the benchmarking instance - otherwise it won't manage to connect.
- Start benchmarking with the following command. Inside `../../config/hbase`, we store the HBase configuration created in the previous step. Benchmarker needs the `hbase-site.xml` with correct master location.
  ```
  for workload in a b c f e d; do echo $(date); sleep 5; bin/ycsb run hbase20 -P workloads/workload${workload} -p operationcount=15000000 -p maxexecutiontime=300 -cp  ../../config/hbase/ -p table=usertable -p columnfamily=family; done
  ```
   Date inside the log will tell the exact time when different phases of the benchmark began and finished.
