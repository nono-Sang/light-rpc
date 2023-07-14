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
    response->set_response(request->request());
    done->Run();
  }
};

class HeteServiceImpl : public HeteService {
 public:
  void Hete(google::protobuf::RpcController *controller,
            const TestRequest *request,
            TestResponse *response,
            google::protobuf::Closure *done) override {
    int sleep_time = request->sleep_time();
    sleep(sleep_time);
    response->set_response(request->request());
    done->Run();
  }
};


int main(int argc, char *argv[]) {
  int num_cpus = std::thread::hardware_concurrency();
  std::cout << "Number of CPUs: " << num_cpus << std::endl;

  std::string local_ip;
  GetLocalIp(local_ip);

  int num_threads = std::min(std::max(2, num_cpus / 4), 8);
  std::cout << "There are " << num_threads << " threads in thread pool." << std::endl;
  lightrpc::ResourceConfig config(local_ip, 1024, num_threads, lightrpc::BUSY_POLLING);

  lightrpc::LightServer server(config);
  EchoServiceImpl echo_service;
  HeteServiceImpl hete_service;
  server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
  server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &hete_service);
  server.BuildAndStart();
  return 0;
}