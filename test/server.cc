#include <cmath>
#include <iostream>
#include <thread>

#include "light_control.h"
#include "light_server.h"
#include "test.pb.h"
#include "util.h"

class EchoServiceImpl : public EchoService {
 public:
  void Echo(google::protobuf::RpcController *controller,
            const TestRequest *request,
            TestResponse *response,
            google::protobuf::Closure *done) override {
    std::string res_str(128, 'A');
    response->set_response(res_str);
    // response->set_response(request->request());
    done->Run();
  }
};

int main(int argc, char *argv[]) {
  int num_cpus = std::thread::hardware_concurrency();
  std::cout << "Number of CPUs: " << num_cpus << std::endl;

  std::string local_ip;
  GetLocalIp(local_ip);

  lightrpc::ResourceConfig config = {.local_ip = local_ip,
                                     .local_port = 1024,
                                     .block_pool_size = 100 * 1024 * 1024,
                                     .num_threads = num_cpus};

  lightrpc::LightServer server(config);
  EchoServiceImpl echo_service;
  server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
  server.BuildAndStart();
  return 0;
}