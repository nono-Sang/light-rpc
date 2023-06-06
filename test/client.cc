#include "light_channel.h"
#include "light_control.h"
#include "test.pb.h"
#include "util.h"

void SyncRpcCall(EchoService_Stub &stub);
void AsyncRpcCall(EchoService_Stub &stub);
void SyncMultiThread(EchoService_Stub &stub);
void AsyncMultiThread(EchoService_Stub &stub);

void SyncRpcCall(EchoService_Stub &stub, int freq, int internal);
void SyncMultiThread(EchoService_Stub &stub, int msg_size, int threads_num, int freq, int internal);

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
  SyncRpcCall(stub, atoi(argv[1]), atoi(argv[2]));
  return 0;
}

void SyncRpcCall(EchoService_Stub &stub, int freq, int internal) {
  // // 8 bytes -> 4M bytes
  int init_size = 8;
  int epoch_num = 20;

  auto msglen = std::make_unique<uint32_t[]>(epoch_num);
  auto avglat = std::make_unique<float[]>(epoch_num);
  auto timers = std::make_unique<timespec[]>(epoch_num);

  for (int i = 0; i < epoch_num; i++) {
    msglen[i] = 0;
    avglat[i] = 0;
    memset(&timers[i], 0, sizeof(timespec));
  }

  timespec start, end;
  std::cout << "req size\tmsg size" << std::endl;

  for (int i = 0; i < epoch_num; i++) {
    TestRequest request;
    TestResponse response;
    std::string req_str(init_size, 'A');
    request.set_request(req_str);
    msglen[i] = request.ByteSizeLong();
    std::cout << req_str.size() << "\t\t" << msglen[i] << std::endl;
    for (int j = 1; j <= freq; j++) {
      lightrpc::LightController cntl;
      clock_gettime(CLOCK_REALTIME, &start);
      stub.Echo(&cntl, &request, &response, nullptr);
      clock_gettime(CLOCK_REALTIME, &end);
      time_add(&timers[i], time_diff(start, end));
      if (internal) {
        usleep(internal * 1000);
      }
    }
    init_size *= 2;
    avglat[i] = static_cast<float>(time_avg(timers[i], freq) / 1e3);  // ns -> us
  }

  FILE *fp = fopen("sync_latency.txt", "w");
  for (int i = 0; i < epoch_num; i++) {
    fprintf(fp, "%u %.2f", msglen[i], avglat[i]);
    if (i != epoch_num - 1) fprintf(fp, "\n");
  }
  fclose(fp);
}