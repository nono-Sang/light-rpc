#include "light_channel.h"
#include "light_control.h"
#include "light_log.h"
#include "test.pb.h"
#include "util.h"

const int basic_test_num = 100;
const int sleep_time_s = 10;

void compare(TestRequest* req, TestResponse* res) {
  std::unique_ptr<TestRequest> req_guard(req);
  std::unique_ptr<TestResponse> res_guard(res);
  CHECK(req->request() == res->response());
}

void preheat(EchoService_Stub &stub) {
  int call_times = 10000;
  TestRequest request;
  TestResponse response;
  std::string req_str(10, '#');
  request.set_request(req_str);
  for (int i = 1; i <= call_times; i++) {
    lightrpc::LightController cntl;
    stub.Echo(&cntl, &request, &response, nullptr);
  }
}

/// NOTE: sync rpc call
void test1(EchoService_Stub &stub) {
  for (int i = 1; i <= basic_test_num; i++) {
    TestRequest request;
    TestResponse response;
    std::string req_str(i, '#');
    request.set_request(req_str);
    lightrpc::LightController cntl;
    stub.Echo(&cntl, &request, &response, nullptr);
    CHECK(request.request() == response.response());
  }
  std::cout << "### Test 1 pass ###" << std::endl;
}

/// NOTE: async rpc call
void test2(EchoService_Stub &stub) {
  for (int i = 1; i <= basic_test_num; i++) {
    auto request = new TestRequest;
    auto response = new TestResponse;
    auto done = google::protobuf::NewCallback(
      &compare, request, response);
    std::string req_str(i, '#');
    request->set_request(req_str);
    lightrpc::LightController cntl;
    stub.Echo(&cntl, request, response, done);
  }
  sleep(sleep_time_s);
  std::cout << "### Test 2 pass ###" << std::endl;
}

/// NOTE: multi thread sync rpc call
void test3(EchoService_Stub &stub) {
  int num_thread = 3;
  std::vector<std::thread> th_vec(num_thread);
  for (int k = 0; k < num_thread; k++) {
    th_vec[k] = std::thread([&stub] {
      for (int i = 1; i <= basic_test_num; i++) {
        TestRequest request;
        TestResponse response;
        std::string req_str(i, '#');
        request.set_request(req_str);
        lightrpc::LightController cntl;
        stub.Echo(&cntl, &request, &response, nullptr);
        CHECK(request.request() == response.response());
      }
    });
  }
  for (int i = 0; i < num_thread; i++) {
    th_vec[i].join();
  }
  std::cout << "### Test 3 pass ###" << std::endl;
}

/// NOTE: multi thread async rpc call
void test4(EchoService_Stub &stub) {
  int num_thread = 3;
  std::vector<std::thread> th_vec(num_thread);
  for (int k = 0; k < num_thread; k++) {
    th_vec[k] = std::thread([&stub] {
      for (int i = 1; i <= basic_test_num; i++) {
        auto request = new TestRequest;
        auto response = new TestResponse;
        auto done = google::protobuf::NewCallback(
          &compare, request, response);
        std::string req_str(i, '#');
        request->set_request(req_str);
        lightrpc::LightController cntl;
        stub.Echo(&cntl, request, response, done);
      }
    });
  }
  for (int i = 0; i < num_thread; i++) {
    th_vec[i].join();
  }
  sleep(sleep_time_s);
  std::cout << "### Test 4 pass ###" << std::endl;
}

/// NOTE: data size from 8 bytes to 4M bytes
void test5(EchoService_Stub &stub) {
  int data_size = 8;
  int end_size = 4 * 1024 * 1024;
  while (data_size <= end_size) {
    for (int i = 1; i <= basic_test_num; i++) {
      TestRequest request;
      TestResponse response;
      std::string req_str(i, '#');
      request.set_request(req_str);
      lightrpc::LightController cntl;
      stub.Echo(&cntl, &request, &response, nullptr);
      CHECK(request.request() == response.response());
    }
    data_size *= 2;
  }
  std::cout << "### Test 5 pass ###" << std::endl;
}

/// NOTE: heterogeneous tasks
void test6(HeteService_Stub &stub) {
  int max_sleep_time = 20;
  for (int i = 1; i <= max_sleep_time; i++) {
    auto request = new TestRequest;
    auto response = new TestResponse;
    auto done = google::protobuf::NewCallback(
      &compare, request, response);
    std::string req_str(i, '#');
    request->set_request(req_str);
    request->set_sleep_time(i);
    lightrpc::LightController cntl;
    stub.Hete(&cntl, request, response, done);
  }
  sleep(100);
}

void SyncLatencyTest(EchoService_Stub& stub, int freq = 10000) {
  int data_size = 8;
  int end_size = 16 * 1024 * 1024;

  std::vector<uint32_t> msglen_vec;
  std::vector<float> avglat_vec;
  timespec start, end;
  std::cout << "req size" << "  " << "msg size" << std::endl;

  while (data_size <= end_size) {
    TestRequest request;
    TestResponse response;
    std::string req_str(data_size, '#');
    request.set_request(req_str);
    uint32_t msglen = request.ByteSizeLong();
    msglen_vec.emplace_back(msglen); 
    std::cout << req_str.size() << "  " << msglen << std::endl;
    
    if (data_size >= 1024 * 1024) freq = 5000;
    clock_gettime(CLOCK_REALTIME, &start);
    for (int i = 1; i <= freq; i++) {
      lightrpc::LightController cntl;
      stub.Echo(&cntl, &request, &response, nullptr);
    }
    clock_gettime(CLOCK_REALTIME, &end);
    avglat_vec.emplace_back(static_cast<float>(
      time_avg(time_diff(start, end), freq) / 1e3));  // ns -> us
    data_size *= 2;
  }

  FILE *fp = fopen("latency.txt", "w");
  int len = msglen_vec.size();
  for (int i = 0; i < len; i++) {
    fprintf(fp, "%u %.2f\n", msglen_vec[i], avglat_vec[i]);
  }
  fclose(fp);
}

void MultiThreadTest(EchoService_Stub& stub, int freq, int data_size, int internal) {
  int num_thread_vec[7] = {1, 2, 4, 8, 16, 32, 48};
  std::vector<float> total_time_vec(7);  // us
  std::vector<float> lty_99_vec(7), lty_mean_vec(7);  // us
  std::vector<uint32_t> total_call_vec(7);
  std::vector<float> throughput_vec(7);  // k/s

  for (int i = 0; i < 7; i++) {
    int num_threads = num_thread_vec[i];
    std::cout << "the number of threads is: " << num_threads << std::endl;
    std::vector<std::thread> thread_vec(num_threads);
    std::vector<timespec> time_vec(num_threads); 
    std::vector<std::vector<uint64_t>> lty_vec(num_threads, std::vector<uint64_t>(freq)); 
    for (int i = 0; i < num_threads; i++) memset(&time_vec[i], 0, sizeof(timespec));
    for (int k = 0; k < num_threads; k++) {
      thread_vec[k] = std::thread(
        [k, &time_vec, &lty_vec, &stub, freq, data_size, internal] {
        TestRequest request;
        TestResponse response;
        std::string req_str(data_size, '#');
        request.set_request(req_str);

        timespec start, end;
        for (int t = 0; t < freq; t++) {
          clock_gettime(CLOCK_REALTIME, &start);
          lightrpc::LightController cntl;
          stub.Echo(&cntl, &request, &response, nullptr);
          clock_gettime(CLOCK_REALTIME, &end);
          time_add(&time_vec[k], time_diff(start, end));
          lty_vec[k][t] = time_avg(time_diff(start, end), 1);
          if(internal) usleep(internal);  // us
        }
      });
    }

    for (int k = 0; k < num_threads; k++) {
      thread_vec[k].join();
    }

    std::vector<uint64_t> all_lty_vec(num_threads * freq);
    for (int k = 0; k < num_threads; k++) {
      for (int t = 0; t < freq; t++) {
        all_lty_vec[k * freq + t] = lty_vec[k][t];
      }
    }
    std::sort(all_lty_vec.begin(), all_lty_vec.end());
    int idx_99 = (num_threads * freq - 1) * 0.99;
    lty_99_vec[i] = all_lty_vec[idx_99] / 1000.0;
    uint64_t all_lty_sum = 0;

    timespec max_time;
    memset(&max_time, 0, sizeof(timespec));
    for (auto& val : time_vec) {
      if (time_compare(val, max_time)) max_time = val;
      all_lty_sum += time_avg(val, 1);
    }

    float total_time = time_avg(max_time, 1) / 1e3;
    uint64_t total_call = num_threads * freq;
    float throughput = total_call * 1e3 / total_time;

    total_time_vec[i] = total_time;
    total_call_vec[i] = total_call;
    throughput_vec[i] = throughput;
    lty_mean_vec[i] = all_lty_sum / 1000.0 / total_call;
  }

  std::string file_name = std::to_string(data_size) + "_throughput.txt";
  FILE *fp = fopen(file_name.c_str(), "w");
  for (int i = 0; i < 7; i++) {
    // fprintf(fp, "%.3f\n", total_time_vec[i]);
    // fprintf(fp, "%u\n", total_call_vec[i]);
    fprintf(fp, "%.3f %.3f %.3f", throughput_vec[i], lty_mean_vec[i], lty_99_vec[i]);
    if (i != 6) fprintf(fp, "\n");
  }
  fclose(fp);
}


int main(int argc, char *argv[]) {
  int num_cpus = std::thread::hardware_concurrency();
  std::cout << "Number of CPUs: " << num_cpus << std::endl;

  std::string local_ip;
  GetLocalIp(local_ip);

  int num_threads = std::min(std::max(2, num_cpus / 4), 8);
  std::cout << "There are " << num_threads << " threads in thread pool." << std::endl;
  lightrpc::ResourceConfig config(local_ip, -1, num_threads, lightrpc::BUSY_POLLING);

  std::string server_ip;
  std::cout << "Input the server IP address: ";
  std::cin >> server_ip;

  lightrpc::ClientGlobalResource global_res(config);
  lightrpc::LightChannel channel(server_ip, 1024, &global_res);

  EchoService_Stub echo_stub(&channel);
  HeteService_Stub hete_stub(&channel);
  preheat(echo_stub);
  // test1(echo_stub);
  // test2(echo_stub);
  // test3(echo_stub);
  // test4(echo_stub);
  // test5(echo_stub);
  // test6(hete_stub);

  SyncLatencyTest(echo_stub);

  // int internal = 0;  // us
  // MultiThreadTest(echo_stub, 10000, 512, internal);
  // MultiThreadTest(echo_stub, 5000, 4 * 1024, internal);
  // MultiThreadTest(echo_stub, 5000, 512 * 1024, internal);
  // MultiThreadTest(echo_stub, 500, 4 * 1024 * 1024, internal);
  return 0;
}