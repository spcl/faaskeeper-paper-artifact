# FaaSKeeper Artifact

### [Main FaaSKeeper repository - source code, deployment instructions, documentation](https://github.com/spcl/faaskeeper)

We evaluate FaaSKeeper with a collection of benchmarks. Each section of this document describes each experiment separately. The repository consists of three directories:
* `benchmarks` with benchmarking code and invocation scripts,
* `data` that contains data we generated for the paper,
* `analysis` with Jupyter notebooks that compute statistics and generate plots used in the paper.

Before running any experiments, run the script `install.py`. It will create a virtual environment, `python-venv,` which you need to activate before running any experiments.
This Python virtual environment ensures that all required dependencies are present to run experiments.

> [!IMPORTANT]  
> All benchmarks are designed to be executed from virtual machines in the cloud, either in AWS or GCP. Since both the latency and throughput depends on the proximity to native cloud resources and services, running them from your local machine will not produce accurate results.

## Microbenchmarks

### Storage

**Directory**: `microbenchmark_storage`.

For this benchmark, we evaluate the read and write performance of different storage options on AWS. Use the scripts `run_intra_region.sh` and `run_inter_region.sh` to obtain data access performance in the same or different AWS region, respectively. Jupyter notebooks generate Figure 3 from the paper, Section 4.2. 

### Synchronization Primitives

**Directory**: `microbenchmark_primitives`.

This benchmark evaluates the latency of throughput of using atomic and lock operations on AWS DynamoDB. You can use the `run.sh` script to generate latency results, while `run_throughput.sh` and `run_throughput_locked.sh` evaluate the throughput performance using locked and standard update operations. Jupyter notebooks generate Figure 5 in the paper.

### Queue

**Directory**: `microbenchmark_queue`.

In this benchmark, we evaluate the latency of throughput of invoking serverless functions with a queue trigger. You can use the `run_latency.sh` and `run_throughput.sh` scripts to generate latency and throughput results, respectively. Jupyter notebooks generate Figure 6 in the paper.

> [!IMPORTANT]  
> In this benchmark, the function established a TCP connection to the benchmarking host to send a confirmation. Therefore, it must be run from a virtual machine that can accept incoming TCP connections. Thus, you need to change the security rules of your virtual machine to allow incoming connections on port 60000. The port can be changed in the benchmarking script.

## Benchmarks

### ZooKeeper Read

**Directory**: `read`.

This benchmark evaluates read performance for a FaaSKeeper instance on AWS and GCP. Additionally, it evaluates ZooKeeper read performance on both clouds.

Change the settings in `test_read.json` to evaluate different user storage options, such as `persistent`, `key-value`, and `redis`. Use the `benchmark.py` to run the benchmark; it accepts flags such as `repetitions` and `size` to change the number of samples produced and ZK node size.

Jupyter notebooks generates Figure 7 in the paper.

### Write

This benchmark evaluates write performance for a FaaSKeeper instance. Change the settings in `test_write.json` to evaluate different system and user storage options, such as `persistent`, `key-value`, and `redis`. Use the `benchmark.py` to run the benchmark; it accepts flags such as `repetitions` and `size` to change the number of samples produced and ZK node size.

Jupyter notebook `final_plots.ipynb` generate Figures 9 (write performance on AWS), 10 (write distribution on AWS), 11 (hybrid storage on AWS) and 12 (write on GCP) in the paper, as well as data for Table 3 (write variability on AWS).

The data processing for GCP vCPU performance can be found in the notebook `gcp_vcpus.ipynb`.

The data processing for AWS ARM functions can be found in the notebook `lambda_arm.ipynb`.

### Heartbeat

This benchmark evaluates the performance of sending parallel heartbeat notifications to different FaaSKeeper clients. Use the `benchmark.py` to run the benchmark; it accepts flags such as `repetitions` and `clients` to change the number of samples produced and number of clients to be notified. The script will generate multiple FK client processes and run the heartbeat function in the cloud. Jupyter notebook generates Figure 13 in the paper.

> [!IMPORTANT]  
> In this benchmark, the function established a TCP connection to the benchmarking host to send a confirmation. Therefore, it must be run from a virtual machine that can accept incoming TCP connections. Thus, you need to change the security rules of your virtual machine to allow incoming connections on port 60000. The port can be changed in the benchmarking script.

### ZooKeeper Performance

The benchmarks `read/zookeeper_read` and `zookeeper_write` evaluates the ZooKeeper read and write performance, respectively, which is displayed in the Figures 8 and 9 in the paper. To use them, first deploy ZooKeeper on three different VMs, using the configuration files provided in `zookeeper_deployment`. In each configuration file, you need to change two of three available IPs (omit the `0.0.0.0` address) by putting the IP addresses of your VMs.

Then, each file `run.sh` will execute a ZooKeeper client performing read or write operations. It is advisable to put the virtual machine in the same cloud region.

> [!NOTE]  
> To increase deployment reliability, ZooKeeper instances should be put in different availabiliy zones of the same cloud region. Please check the documentation of your cloud provider how to achieve that.

### ZooKeeper Utilization

**Directory**: `zookeeper_utilization`.

This benchmarks measures the utilization of ZooKeeper when hosting a Solr instance:

1) Run the `install_zk.sh` script on each VM to download and install a ZooKeeper instance.
2) On each VM, run `g++ benchmarks/zookeeper_utilization/profiler.cpp -o profiler.x` to compile a dedicated profiling system.
3) The `generate_zk_cfg.sh` script and provide three arguments corresponding to VM IPs.
4) Run the `start_zk.sh` script - it's going to copy the ZK configuration files to the VM, SSH there and start a ZK instance.
5) Run the `start_profiler.sh` script - it's going to SSH to each machine and start profiling script.
6) Run the Solr workload.
7) Run the `stop_profiler.sh` script- it's going to SSH to each machine, stop profiler, copy results to benchmarking machine and query ZK tree to generate statistics on node counts and sizes.
deploy ZooKeeper on three different VMs, using the configuration files provided in `zookeeper_deployment`. In each configuration file, you need to change two of three available IPs (omit the `0.0.0.0` address) by putting the IP addresses of your VMs.

Jupyter notebook in `analysis/zookeeper_utilization` generates Figure 4 in the paper.

### Cost Analysis

The Jupyter notebook in `analysis/cost_analysis` conducts a cost analysis of FaaSKeeper. It will create the Figure 14 in paper.
