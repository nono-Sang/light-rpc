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
  std::string local_ip;
  GetLocalIp(local_ip);

  lightrpc::ResourceConfig config(local_ip, 1024);

  lightrpc::LightServer server(config);
  EchoServiceImpl echo_service;
  HeteServiceImpl hete_service;
  server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
  server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &hete_service);
  server.BuildAndStart();
  return 0;
}