#include <mpi.h>

#include "light_channel.h"
#include "light_control.h"
#include "light_server.h"
#include "test.pb.h"
#include "util.h"

const int send_thread_num = 4;

class FixRepServiceImpl : public FixRepService {
 public:
  void FixRep(google::protobuf::RpcController *controller,
              const TestRequest *request,
              TestResponse *response,
              google::protobuf::Closure *done) override {
    response->set_response("ok");
    done->Run();
  }
};

// argv[1]: data size
// argv[2]: frequency
// argv[3]: server ip

int main(int argc, char *argv[]) 
{
  int size, rank;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::cout << "Init over..." << std::endl;

  if (argc != 4) {
    fprintf(stderr, "Input Format: ./mpi_test data_size frequency server_ip\n");
    exit(EXIT_FAILURE);
  }

  int data_size = atoi(argv[1]);
  int frequency = atoi(argv[2]);
  std::string server_ip = argv[3];

  int color = -1;
  if (rank == 0) color = 0;
  else color = 1;

  MPI_Comm new_comm;
  MPI_Comm_split(MPI_COMM_WORLD, color, rank, &new_comm);

  std::string local_ip;
  GetLocalIp(local_ip);
  int num_cpus = std::thread::hardware_concurrency();
  int num_threads = std::min(std::max(2, num_cpus / 4), 8);

  if (rank == 0) {
    // Server Operations.
    lightrpc::ResourceConfig config = {
      .local_ip = local_ip, .local_port = 1024, .num_threads = num_cpus };
    lightrpc::LightServer server(config);
    FixRepServiceImpl fixrep_service;
    server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &fixrep_service);
    server.BuildAndStart();
  } else {
    // Client Operations.
    sleep(3);
    float rank_latency = 0.0, max_latency = 0.0;
    int client_rank;
    MPI_Comm_rank(new_comm, &client_rank);
    lightrpc::ResourceConfig config = {
      .local_ip = local_ip, .local_port = -1, .num_threads = num_threads };
    
    lightrpc::ClientGlobalResource global_res(config);
    lightrpc::LightChannel channel(server_ip, 1024, &global_res);
    FixRepService_Stub stub(&channel);

    // System Preheating.
    TestRequest request;
    TestResponse response;
    std::string req_str(data_size, '#');
    request.set_request(req_str);
    for (int i = 1; i <= 500; i++) {
      lightrpc::LightController cntl;
      stub.FixRep(&cntl, &request, &response, nullptr);
    }
    MPI_Barrier(new_comm);

    timespec start, end;
    std::vector<std::thread> thread_vec(send_thread_num);
    clock_gettime(CLOCK_REALTIME, &start);
    for (int t = 0; t < send_thread_num; t++) {
      thread_vec[t] = std::thread([&stub, frequency, data_size] {
        for (int i = 1; i <= frequency; i++) {
          TestRequest request;
          TestResponse response;
          std::string req_str(data_size, '#');
          request.set_request(req_str);
          lightrpc::LightController cntl;
          stub.FixRep(&cntl, &request, &response, nullptr);
        }
      });
    }
    for (int t = 0; t < send_thread_num; t++) {
      thread_vec[t].join();
    }
    clock_gettime(CLOCK_REALTIME, &end);
    MPI_Barrier(new_comm);

    rank_latency = static_cast<float>(time_avg(time_diff(start, end), 1) / 1e3);
    MPI_Reduce(&rank_latency, &max_latency, 1, MPI_FLOAT, MPI_MAX, 0, new_comm);

    if (client_rank == 0) {
      uint64_t total_call = frequency * (size - 1);
      float throughput = total_call * 1e3 / max_latency;
      fprintf(stderr, "The total time is %.3f us.\n", max_latency);
      fprintf(stderr, "The total call is %ld.\n", total_call);
      fprintf(stderr, "The throughput is %.3f k/s. \n", throughput);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}