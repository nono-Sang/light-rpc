#include <mpi.h>

#include "light_channel.h"
#include "light_server.h"
#include "light_control.h"
#include "light_log.h"
#include "test.pb.h"
#include "util.h"

const int num_warmup_call = 10000;

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

void Warmup(EchoService_Stub &stub) {
  TestRequest request;
  TestResponse response;
  std::string req_str(10, '#');
  request.set_request(req_str);
  for (int i = 1; i <= num_warmup_call; i++) {
    lightrpc::LightController cntl;
    stub.Echo(&cntl, &request, &response, nullptr);
  }
}

uint64_t MultiThreadTest(EchoService_Stub& stub, int data_size, 
                         int duration = 10, int num_threads = 32) {
  std::vector<uint64_t> rpc_count_vec(num_threads, 0);
  std::vector<std::thread> thread_vec(num_threads);

  for (int k = 0; k < num_threads; ++k) {
    thread_vec[k] = std::thread([k, data_size, duration, &stub, &rpc_count_vec] {
      TestRequest request;
      TestResponse response;
      std::string req_str(data_size, '#');
      request.set_request(req_str);
      // Get start time and compute end time.
      timespec start, end, curr;
      clock_gettime(CLOCK_REALTIME, &start);
      end.tv_sec = start.tv_sec + duration;
      end.tv_nsec = start.tv_nsec;
      while (true) {
        lightrpc::LightController cntl;
        stub.Echo(&cntl, &request, &response, nullptr);
        rpc_count_vec[k] += 1;
        // Get current time and judge if break.
        clock_gettime(CLOCK_REALTIME, &curr);
        if (time_compare(curr, end)) break;
      }
    });
  }
  for (int k = 0; k < num_threads; ++k) {
    if (thread_vec[k].joinable()) {
      thread_vec[k].join();
    } else {
      LOG_ERR("Can't join.");
    }
  }
  // Compute the QPS.
  uint64_t qps = 0;
  for (auto& cnt : rpc_count_vec) qps += cnt;
  return qps / duration;
}


// argv[1]: server ip
int main(int argc, char* argv[])
{
  int size, rank;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::cout << "Init over..." << std::endl;

  int color = -1;
  if (rank == 0) color = 0;
  else color = 1;
  MPI_Comm new_comm;
  MPI_Comm_split(MPI_COMM_WORLD, color, rank, &new_comm);

  std::string local_ip;
  GetLocalIp(local_ip);
  std::string server_ip = argv[1];

  if (rank == 0) {
    lightrpc::ResourceConfig config(local_ip, 1024);
    lightrpc::LightServer server(config);
    EchoServiceImpl echo_service;
    server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
    server.BuildAndStart();
  } else {
    sleep(5);
    int new_rank;
    MPI_Comm_rank(new_comm, &new_rank);

    lightrpc::ResourceConfig config(local_ip);
    lightrpc::ClientGlobalResource global_res(config);
    lightrpc::LightChannel channel(server_ip, 1024, &global_res);
    EchoService_Stub echo_stub(&channel);

    Warmup(echo_stub);

    std::vector<int> data_size_vec = {512, 4 * 1024, 512 * 1024, 4 * 1024 * 1024};
    std::vector<uint64_t> total_qps_vec(data_size_vec.size());
    for (size_t i = 0; i < data_size_vec.size(); ++i) {
      MPI_Barrier(new_comm);
      uint64_t rank_qps = MultiThreadTest(echo_stub, data_size_vec[i]);
      MPI_Reduce(&rank_qps, &total_qps_vec[i], 1, MPI_UINT64_T, MPI_SUM, 0, new_comm);
    }

    if (new_rank == 0) {
      std::string file_name = std::to_string(size) + "_nodes_qps.txt";
      FILE *fp = fopen(file_name.c_str(), "w");
      for (size_t i = 0; i < data_size_vec.size(); ++i) {
        std::cout << data_size_vec[i] << " " << total_qps_vec[i] << std::endl;
        fprintf(fp, "%d %ld", data_size_vec[i], total_qps_vec[i]);
        if (i != data_size_vec.size() - 1) fprintf(fp, "\n");
      }
      fclose(fp);
    }
  }
  MPI_Finalize();
  return 0;
}