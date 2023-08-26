#include <mpi.h>

#include "light_channel.h"
#include "light_server.h"
#include "light_control.h"
#include "light_log.h"
#include "test.pb.h"
#include "util.h"

const int num_warmup_call = 10000;
const int ip_length = 20;

const int default_data_size = 4 * 1024;
const int default_duration = 10;
const int default_num_threads = 4;

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

uint64_t MultiThreadTest(EchoService_Stub& stub, 
                         int data_size = default_data_size, 
                         int duration = default_duration, 
                         int num_threads = default_num_threads) {
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


int main(int argc, char* argv[])
{
  int size, rank;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  std::cout << "Init over..." << std::endl;

  std::string local_ip;
  GetLocalIp(local_ip);

  char** server_ip_ptr = (char**)malloc(size * sizeof(int*));
  for (int i = 0; i < size; ++i) {
    server_ip_ptr[i] = (char*)malloc(ip_length * sizeof(char));
    memset(server_ip_ptr[i], 0, ip_length * sizeof(char));
    if (rank == i) MPI_Bcast((char*)local_ip.c_str(), local_ip.size(), MPI_CHAR, i, MPI_COMM_WORLD);
    else MPI_Bcast(server_ip_ptr[i], local_ip.size(), MPI_CHAR, i, MPI_COMM_WORLD);
  }

  // Init server
  std::thread server_th = std::thread([&local_ip] {
    lightrpc::ResourceConfig config(local_ip, 1024);
    lightrpc::LightServer server(config);
    EchoServiceImpl echo_service;
    server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
    server.BuildAndStart();
  });

  // Init clients
  sleep(5);
  std::vector<std::thread> client_ths(size);
  lightrpc::ResourceConfig config(local_ip);
  lightrpc::ClientGlobalResource global_res(config);

  std::vector<uint64_t> qps_vec(size, 0);
  for (int i = 0; i < size; ++i) {
    if (i == rank) continue;
    std::string goal_ip = server_ip_ptr[i];
    std::cout << "goal ip: " << goal_ip << std::endl;
    client_ths[i] = std::thread([i, &qps_vec, &global_res, &goal_ip] {
      lightrpc::LightChannel channel(goal_ip, 1024, &global_res);
      EchoService_Stub echo_stub(&channel);
      Warmup(echo_stub);
      qps_vec[i] = MultiThreadTest(echo_stub);
    });
  }

  for (int i = 0; i < size; ++i) {
    if (i == rank) continue;
    if (client_ths[i].joinable()) client_ths[i].join();
    else LOG_ERR("Can't join.");
  }

  uint64_t rank_qps = 0;
  for (auto& cnt : qps_vec) rank_qps += cnt;
  uint64_t total_qps = 0;
  MPI_Reduce(&rank_qps, &total_qps, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    std::cout << "***********************************\n"
              << "Data size: " << default_data_size << "\n" 
              << "Num threads each connection: " << default_num_threads << "\n" 
              << "System total qps: " << total_qps << "\n" 
              << "***********************************\n";
  }

  server_th.join();

  for (int i = 0; i < size; ++i) free(server_ip_ptr[i]);
  free(server_ip_ptr);
  return 0;
}