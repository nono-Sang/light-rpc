#include "light_channel.h"
#include "light_control.h"
#include "light_log.h"
#include "test.pb.h"
#include "util.h"

const int num_warmup_call = 10000;
// For latency test.
const int start_bytes = 8;
const int end_bytes = 16 * 1024 * 1024;

void Warmup(EchoService_Stub &stub) {
  TestRequest request;
  TestResponse response;
  std::string req_str(10, '#');
  request.set_request(req_str);
  for (int i = 1; i <= num_warmup_call; i++) {
    lightrpc::LightController cntl;
    stub.Echo(&cntl, &request, &response, nullptr);
  }
}

void LatencyTest(EchoService_Stub& stub) {
  std::vector<uint32_t> msglen_vec;
  std::vector<uint32_t> avglat_vec;
  timespec start, end;
  std::cout << "req_size msg_size" << std::endl;

  for (int data_size = start_bytes; data_size <= end_bytes; data_size *= 2) {
    TestRequest request;
    TestResponse response;
    std::string req_str(data_size, '#');
    request.set_request(req_str);
    uint32_t msglen = request.ByteSizeLong();
    msglen_vec.emplace_back(msglen); 
    std::cout << data_size << " " << msglen << std::endl;
    
    int freq = 0;
    if (data_size < 1024 * 1024) freq = 10000;
    else freq = 5000;

    // Send sync RPC calls.
    clock_gettime(CLOCK_REALTIME, &start);
    for (int i = 1; i <= freq; i++) {
      lightrpc::LightController cntl;
      stub.Echo(&cntl, &request, &response, nullptr);
    }
    clock_gettime(CLOCK_REALTIME, &end);

    avglat_vec.emplace_back(time_avg(time_diff(start, end), freq) / 1e3);  // ns -> us
  }

  FILE *fp = fopen("latency.txt", "w");
  int len = msglen_vec.size();
  for (int i = 0; i < len; i++) {
    fprintf(fp, "%u %u\n", msglen_vec[i], avglat_vec[i]);
  }
  fclose(fp);
}

void MultiThreadTest(EchoService_Stub& stub, int data_size, int duration = 10) {
  std::vector<int> num_th_vec = {1, 2, 4, 8, 16, 32, 64};
  std::vector<std::pair<int, uint64_t>> qps_vec(num_th_vec.size());
  for (size_t i = 0; i < num_th_vec.size(); ++i) {
    int num_threads = num_th_vec[i];
    std::cout << "The number of threads is: " << num_threads << std::endl;
    std::vector<uint64_t> rpc_count_vec(num_threads, 0);
    std::vector<std::thread> thread_vec(num_threads);
    for (int k = 0; k < num_threads; ++k) {
      thread_vec[k] = std::thread([k, data_size, duration, &stub, &rpc_count_vec] {
        TestRequest request;
        TestResponse response;
        std::string req_str(data_size, '#');
        request.set_request(req_str);
        // Get start time and compute end time.
        timespec start, end, curr;
        clock_gettime(CLOCK_REALTIME, &start);
        end.tv_sec = start.tv_sec + duration;
        end.tv_nsec = start.tv_nsec;
        while (true) {
          lightrpc::LightController cntl;
          stub.Echo(&cntl, &request, &response, nullptr);
          rpc_count_vec[k] += 1;
          // Get current time and judge if break.
          clock_gettime(CLOCK_REALTIME, &curr);
          if (time_compare(curr, end)) break;
        }
      });
    }
    for (int k = 0; k < num_threads; ++k) {
      if (thread_vec[k].joinable()) {
        thread_vec[k].join();
      } else {
        LOG_ERR("Can't join.");
      }
    }
    uint64_t qps = 0;
    for (auto& cnt : rpc_count_vec) qps += cnt;
    qps = qps / duration;
    qps_vec[i] = std::make_pair(num_threads, qps);
  }
  std::string file_name = std::to_string(data_size) + "_qps.txt";
  FILE *fp = fopen(file_name.c_str(), "w");
  for (size_t i = 0; i < qps_vec.size(); ++i) {
    fprintf(fp, "%d %ld", qps_vec[i].first, qps_vec[i].second);
    if (i != qps_vec.size() - 1) fprintf(fp, "\n");
  }
  fclose(fp);
}

void MultiConnectionTest(lightrpc::ClientGlobalResource& global_res, std::string& server_ip,
                         int data_size, int duration = 10) {
  std::vector<int> num_th_vec = {1, 2, 4, 8, 16, 32, 64};
  std::vector<std::pair<int, uint64_t>> qps_vec(num_th_vec.size());
  for (size_t i = 0; i < num_th_vec.size(); ++i) {
    int num_threads = num_th_vec[i];
    std::cout << "The number of threads is: " << num_threads << std::endl;
    std::vector<uint64_t> rpc_count_vec(num_threads, 0);
    std::vector<std::thread> thread_vec(num_threads);
    for (int k = 0; k < num_threads; ++k) {
      thread_vec[k] = std::thread([k, data_size, duration, &rpc_count_vec, &server_ip, &global_res] {
        lightrpc::LightChannel channel(server_ip, 1024, &global_res);
        EchoService_Stub stub(&channel);
        TestRequest request;
        TestResponse response;
        std::string req_str(data_size, '#');
        request.set_request(req_str);
        // Get start time and compute end time.
        timespec start, end, curr;
        clock_gettime(CLOCK_REALTIME, &start);
        end.tv_sec = start.tv_sec + duration;
        end.tv_nsec = start.tv_nsec;
        while (true) {
          lightrpc::LightController cntl;
          stub.Echo(&cntl, &request, &response, nullptr);
          rpc_count_vec[k] += 1;
          // Get current time and judge if break.
          clock_gettime(CLOCK_REALTIME, &curr);
          if (time_compare(curr, end)) break;
        }
      });
    }
    for (int k = 0; k < num_threads; ++k) {
      if (thread_vec[k].joinable()) {
        thread_vec[k].join();
      } else {
        LOG_ERR("Can't join.");
      }
    }
    // Compute the QPS.
    uint64_t qps = 0;
    for (auto& cnt : rpc_count_vec) qps += cnt;
    qps = qps / duration;
    qps_vec[i] = std::make_pair(num_threads, qps);
  }
  std::string file_name = std::to_string(data_size) + "_qps_mc.txt";
  FILE *fp = fopen(file_name.c_str(), "w");
  for (size_t i = 0; i < qps_vec.size(); ++i) {
    fprintf(fp, "%d %ld", qps_vec[i].first, qps_vec[i].second);
    if (i != qps_vec.size() - 1) fprintf(fp, "\n");
  }
  fclose(fp);
}

int main(int argc, char *argv[]) {
  std::string local_ip;
  GetLocalIp(local_ip);

  lightrpc::ResourceConfig config(local_ip);

  std::string server_ip;
  std::cout << "Input the server IP address: ";
  std::cin >> server_ip;

  lightrpc::ClientGlobalResource global_res(config);

  lightrpc::LightChannel channel(server_ip, 1024, &global_res);
  EchoService_Stub echo_stub(&channel);

  // system warmup
  Warmup(echo_stub);

  // 1. single latency
  // LatencyTest(echo_stub);

  // 2. multi threads qps
  MultiThreadTest(echo_stub, 1 * 1024 * 1024);
  MultiThreadTest(echo_stub, 2 * 1024 * 1024);
  MultiThreadTest(echo_stub, 4 * 1024 * 1024);
  MultiThreadTest(echo_stub, 8 * 1024 * 1024);

  // 3. multi connections qps
  // MultiConnectionTest(global_res, server_ip, 512);
  // MultiConnectionTest(global_res, server_ip, 4 * 1024);
  // MultiConnectionTest(global_res, server_ip, 512 * 1024);
  // MultiConnectionTest(global_res, server_ip, 4 * 1024 * 1024);
  return 0;
}