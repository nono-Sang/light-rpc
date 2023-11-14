#include "inc/fast_channel.h"
#include "build/test.pb.h"
#include "test/utils.h"

#include <thread>

const int duration_seconds = 10;
const int max_rpc_cnt = 1000000;
std::string local_ip, server_ip;

void Test(int req_bytes, int num_threads, int sleep_us = 0) {
  std::vector<fast::UniqueResource*> single_res_vec;
  std::vector<fast::FastChannel*> small_chan_vec;
  std::vector<EchoService_Stub*> stub_vec;
  for (int i = 0; i < num_threads; ++i) {
    single_res_vec.emplace_back(new fast::UniqueResource(local_ip));
    small_chan_vec.emplace_back(new fast::FastChannel(single_res_vec[i], server_ip, 1024));
    stub_vec.emplace_back(new EchoService_Stub(small_chan_vec[i]));
  }

  const std::string req_str(req_bytes, '#');
  volatile bool stop_flag = false;
  std::vector<std::thread> thread_vec(num_threads);
  std::vector<int64_t*> latency_mat(num_threads);
  std::vector<int64_t> query_cnt_vec(num_threads, 0);

  for (int i = 0; i < num_threads; ++i) {
    latency_mat[i] = new int64_t[max_rpc_cnt];
  }

  int64_t start_time = GET_CURRENT_MS();
  for (int k = 0; k < num_threads; ++k) {
    thread_vec[k] = std::thread([k, &stub_vec, &req_str, &stop_flag, &latency_mat, &query_cnt_vec, sleep_us] {
      auto stub = stub_vec[k];
      int64_t& query_cnt = query_cnt_vec[k];
      int64_t* lat_arr = latency_mat[k];
      int64_t time_1, time_2;
      while (stop_flag == false) {
        time_1 = GET_CURRENT_NS();
        TestRequest request;
        TestResponse response;
        request.set_request(req_str);
        stub->Echo(nullptr, &request, &response, nullptr);
        time_2 = GET_CURRENT_NS();
        lat_arr[query_cnt++] = time_2 - time_1;  // ns
        if (sleep_us) usleep(sleep_us);
      }
    });
  }
  sleep(duration_seconds);
  stop_flag = true;
  int64_t end_time = GET_CURRENT_MS();

  for (int i = 0; i < num_threads; ++i) {
    thread_vec[i].join();
    delete stub_vec[i];
    delete small_chan_vec[i];
    delete single_res_vec[i];
  }

  int64_t total_query = 0, index = 0;
  for (int i = 0; i < num_threads; ++i) {
    total_query += query_cnt_vec[i];
  }
  std::vector<int64_t> lat_record(total_query, 0);
  for (int i = 0; i < num_threads; ++i) {
    for (int j = 0; j < query_cnt_vec[i]; ++j) {
      lat_record[index++] = latency_mat[i][j];
    }
  }
  std::sort(lat_record.begin(), lat_record.end());

  int64_t qps = total_query * 1000 / (end_time - start_time);
  int64_t median_latency = lat_record[(total_query - 1) / 2] / 1000;  // us
  int64_t p99_latency = lat_record[(total_query - 1) * 0.99] / 1000;  // us

  for (int i = 0; i < num_threads; ++i) {
    delete latency_mat[i];
  }

  std::cout << num_threads << " " << qps << " " << median_latency << " " << p99_latency << std::endl;
}

int main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "usage: ./client server_ip\n");
    exit(EXIT_FAILURE);
  }

  GetLocalIp(local_ip);
  server_ip = argv[1];

  // int start_bytes = 32, max_bytes = 1024 * 1024;
  // while (start_bytes <= max_bytes) {
  //   Test(start_bytes, 1, 0);
  //   start_bytes *= 2;
  // }

  // int sleep_us = 0, req_size = 256;
  // std::vector<int> thread_num_vec = {1, 2, 4, 8, 16, 32};
  // for (auto thread_num : thread_num_vec) {
  //   Test(req_size, thread_num, sleep_us);
  // }

  int start_bytes = 32, max_bytes = 1024 * 1024, sleep_us = 0;
  std::vector<int> thread_num_vec = {1, 2, 4, 8, 16, 32, 48};
  while (start_bytes <= max_bytes) {
    std::cout << "message size: " << start_bytes << std::endl;
    for (auto th_num : thread_num_vec) {
      Test(start_bytes, th_num, sleep_us);
    }
    start_bytes *= 2;
    std::cout << std::endl;
  }
  
  return 0;
}