#include "inc/fast_server.h"
#include "build/test.pb.h"
#include "test/utils.h"

const int res_len = 64;
std::string res_str(res_len, '#');

class EchoServiceImpl : public EchoService {
 public:
  void Echo(google::protobuf::RpcController *controller,
            const TestRequest *request,
            TestResponse *response,
            google::protobuf::Closure *done) override {
    response->set_response(request->request());
    // response->set_response(res_str);
    done->Run();
  }
};

int main(int argc, char* argv[])
{
  std::string local_ip;
  GetLocalIp(local_ip);

  fast::SharedResource shared_res(local_ip, 1024);
  fast::FastServer server(&shared_res);

  EchoServiceImpl echo_service;
  server.AddService(fast::SERVER_DOESNT_OWN_SERVICE, &echo_service);
  server.BuildAndStart();
  return 0;
}