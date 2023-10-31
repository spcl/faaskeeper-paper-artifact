
#include <algorithm>
#include <atomic>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>

std::atomic<bool> ending;
char ZK_BUFFER[20000];

struct Measurement
{
  std::chrono::high_resolution_clock::time_point ts;
  uint64_t user, nice, system, idle;
  uint64_t rx_bytes, tx_bytes;
  uint64_t mem_total, mem_free, mem_avail;

  uint64_t zk_read_cnt, zk_write_cnt, zk_namespace_read_cnt, zk_namespace_write_cnt;
};

void signal_handler(int signo)
{
  if (signo == SIGINT) {
    ending.store(true);
  } else {
    std::cerr << "Received unsupported signal " <<  signo << std::endl;
    abort();
  }
}

uint64_t read_netstats(const char* netdev, const char* bytes)
{
  uint64_t val;
  // C++ format not yet available on many compilers 
  std::stringstream ss;
  ss << "/sys/class/net/";
  ss << netdev;
  ss << "/statistics/";
  ss << bytes;
  auto file = fopen(ss.str().c_str(), "r");
  if(file == nullptr) {
    std::cerr << "Couldn't open file " << ss.str() << ", reason: " << errno << " " << strerrordesc_np(errno) << std::endl;
    abort();
  }
  fscanf(file, "%lu", &val);
  fclose(file);
  return val;
}

int zk_connect(int port)
{
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in serverAddress;
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_port = htons(port);
  serverAddress.sin_addr.s_addr = inet_addr("127.0.0.1");

  int ret = connect(sockfd, (struct sockaddr *)&serverAddress, sizeof(serverAddress));
  if(ret != 0) {
    std::cerr << "Connection to ZooKeeper failed, reason: " << errno << " " << strerrordesc_np(errno) << std::endl;
    abort();
  }

  return sockfd;
}

void zk_send(int sockfd)
{
  const char* msg = "mntr";
  int ret = send(sockfd, msg, strlen(msg), 0);
}

uint64_t zk_query(std::string_view stat)
{
  std::string_view buf{ZK_BUFFER};

  auto pos = buf.find(stat);
  if(pos == std::string_view::npos) {
    abort();
  }
  // + 1 to include the whitespace
  pos += stat.length() + 1;

  // find next whitespace
  auto end_pos = buf.find(" ", pos);

  int val;
  auto ret = std::from_chars(ZK_BUFFER + pos, ZK_BUFFER + end_pos, val);

  return val;
}

void zk_recv(int sockfd)
{
  size_t offset = 0;
  while(true) {

    int recv_bytes = recv(sockfd, ZK_BUFFER + offset, sizeof(ZK_BUFFER) - offset, 0);
    offset += recv_bytes;
    if(recv_bytes == 0) {
      break;
    }
    //if(recv_bytes == 4) {
    //  std::cerr << ZK_BUFFER[offset] << std::endl;
    //  std::cerr << ZK_BUFFER[offset+1] << std::endl;
    //  std::cerr << ZK_BUFFER[offset+2] << std::endl;
    //  std::cerr << ZK_BUFFER[offset+3] << std::endl;
    //}
    //std::cerr << recv_bytes << " " << errno << std::endl;
    //if(recv_bytes == 0) {
    //  break;
    //}
  }

  // s

  //printf("Received data from server : %d", recv_bytes);
  //printf("Received data from server : %s", ZK_BUFFER);
  //assert(recv_bytes > 0);
}

void zk_disconnect(int sockfd)
{
  close(sockfd);
}

void main_loop(int frequency, const char* netdev, const char* out_file, int zk_port = -1, const char* zk_namespace = nullptr)
{
  std::vector<Measurement> measurements;
  char cpu[4];
  char mem[32];

  int sockfd = -1;
  //if(zk_port != -1) {
  //  sockfd = connect_zk(zk_port);
  //}

  while(!ending.load()) {

    auto begin = std::chrono::high_resolution_clock::now();
    Measurement msr;
    msr.ts = begin;

    if(zk_port != -1) {
      sockfd = zk_connect(zk_port);
      zk_send(sockfd);
    }

    auto file = fopen("/proc/stat", "r");
    fscanf(file, "%s %lu %lu %lu %lu", cpu, &msr.user, &msr.nice, &msr.system, &msr.idle);
    fclose(file);

    file = fopen("/proc/meminfo", "r");
    fscanf(file, "%s %lu %s", mem, &msr.mem_total, cpu);
    fscanf(file, "%s %lu %s", mem, &msr.mem_free, cpu);
    fscanf(file, "%s %lu %s", mem, &msr.mem_avail, cpu);
    fclose(file);

    msr.rx_bytes = read_netstats(netdev, "rx_bytes");
    msr.tx_bytes = read_netstats(netdev, "tx_bytes");

    if(zk_port != -1) {
      zk_recv(sockfd);
      msr.zk_write_cnt = zk_query("zk_cnt_updatelatency");
      msr.zk_read_cnt = zk_query("zk_cnt_readlatency");
      if(zk_namespace) {
        msr.zk_namespace_write_cnt = zk_query(std::string{"zk_cnt_"} + zk_namespace + "_write_per_namespace");
        msr.zk_namespace_read_cnt = zk_query(std::string{"zk_cnt_"} + zk_namespace + "_read_per_namespace");
      }
      zk_disconnect(sockfd);
    }

    measurements.push_back(std::move(msr));
    auto end = std::chrono::high_resolution_clock::now();

    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end-begin);
    std::this_thread::sleep_for(std::chrono::microseconds(frequency*1000 - 60) - dur);
  }

  std::ofstream output_file{out_file};
  output_file << "timestamp, cpu_user, cpu_nice, cpu_system, cpu_idle, ";
  output_file << "mem_total, mem_free, mem_avail, ";
  output_file << netdev << "_rx_bytes, ";
  output_file << netdev << "_tx_bytes, ";
  output_file << "zk_reads, ";
  output_file << "zk_writes";
  if(zk_namespace) {
    output_file << ", zk_reads_" << zk_namespace << ", ";
    output_file << "zk_writes_" << zk_namespace << "\n";
  } else {
    output_file << "\n";
  }

  auto ts_begin = measurements.front().ts;

  for(auto & msr : measurements) {

    //output_file << std::chrono::duration_cast<std::chrono::microseconds>(msr.ts - ts_begin).count() << ", ";
    output_file << msr.ts.time_since_epoch().count() << ", ";
    output_file << msr.user << ", ";
    output_file << msr.nice << ", ";
    output_file << msr.system << ", ";
    output_file << msr.idle << ", ";
    output_file << msr.mem_total << ", ";
    output_file << msr.mem_free << ", ";
    output_file << msr.mem_avail << ", ";
    output_file << msr.rx_bytes << ", ";
    output_file << msr.tx_bytes << ", ";
    output_file << msr.zk_read_cnt << ", ";
    output_file << msr.zk_write_cnt;
    if(zk_namespace) {
      output_file << ", " << msr.zk_namespace_read_cnt << ", ";
      output_file << msr.zk_namespace_write_cnt;
    } 

    output_file << '\n';
  }

  output_file.close();

  if(zk_port != -1) {
    //disconnect_zk(sockfd);
  }
}

int main(int argc, char ** argv)
{
  if(argc < 4 && argc > 6) {
    std::cerr << "profiler.x <frequency> <net-dev> <output-file> [<zookeeper-port>] [<zookeeper-namespace>]" << std::endl;
    return 1;
  }

  ending.store(false);
  std::signal(SIGINT, signal_handler);

  int frequency = std::stoi(argv[1]);

  if(argc == 4) {
    main_loop(frequency, argv[2], argv[3]);
  } else if(argc == 5) {
    main_loop(frequency, argv[2], argv[3], std::stoi(argv[4]));
  } else {
    main_loop(frequency, argv[2], argv[3], std::stoi(argv[4]), argv[5]);
  }

  return 0;
}
