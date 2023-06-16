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
  while (data_size <= 4 * 1024 * 1024) {
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


int main(int argc, char *argv[]) {
  int num_cpus = std::thread::hardware_concurrency();
  std::cout << "Number of CPUs: " << num_cpus << std::endl;

  std::string local_ip;
  GetLocalIp(local_ip);

  int num_threads = std::min(std::max(2, num_cpus / 4), 8);
  lightrpc::ResourceConfig config = {.local_ip = local_ip,
                                     .local_port = -1,
                                     .block_pool_size = 100 * 1024 * 1024,
                                     .num_threads = num_threads};

  std::string server_ip;
  std::cout << "Input the server IP address: ";
  std::cin >> server_ip;

  lightrpc::ClientGlobalResource global_res(config);
  lightrpc::LightChannel channel(server_ip, 1024, &global_res);

  EchoService_Stub stub(&channel);
  test1(stub);
  test2(stub);
  test3(stub);
  test4(stub);
  test5(stub);
  return 0;
}