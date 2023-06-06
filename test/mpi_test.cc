#include <mpi.h>

#include "light_channel.h"
#include "light_control.h"
#include "light_server.h"
#include "test.pb.h"

class EchoServiceImpl : public EchoService {
 public:
  void Echo(google::protobuf::RpcController *controller,
            const TestRequest *request,
            TestResponse *response,
            google::protobuf::Closure *done) override {
    // std::string res_str(128, 'A');
    // response->set_response(res_str);
    response->set_response(request->request());
    done->Run();
  }
};

int main(int argc, char *argv[]) 
{
  int size, rank;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);


  return 0; 
}