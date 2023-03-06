#include "src/light_controller.h"
#include "src/light_server.h"
#include "echo.pb.h"
#include "util.h"

#include <iostream>
#include <cmath>
#include <thread>

class EchoServiceImpl : public EchoService {
public:
  void Echo(google::protobuf::RpcController* controller,
            const EchoRequest* request,
            EchoResponse* response,
            google::protobuf::Closure* done) override {
    // std::string res_str(128, 'A');
    response->set_message(request->message());
    done->Run();
  }
};

int main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "Input Format: ./server num_clients\n");
    exit(EXIT_FAILURE);
  }

  int num_cpus = std::thread::hardware_concurrency();
  std::cout << "Number of CPUs: " << num_cpus << std::endl;

  std::string local_ip;
  GetLocalIp(local_ip);

  // int num_threads = std::min(std::max(2, num_cpus / 4), 8);
  lightrpc::ResourceConfig config = {
    .local_ip = local_ip,
    .local_port = 1024,
    .num_threads = 1,
    .poll_mode = lightrpc::BUSY_POLLING,
    .block_pool_size = 6 * 1024 * 1024
  };

  lightrpc::LightServer server(config, atoi(argv[1]));
  EchoServiceImpl echo_service;
  server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
  server.BuildAndStart();
  return 0;
}