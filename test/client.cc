#include "src/light_controller.h"
#include "src/light_channel.h"
#include "echo.pb.h"
#include "util.h"

#include <iostream>
#include <thread>
#include <unordered_map>

void HandleResponse(EchoResponse* response) {
  std::unique_ptr<EchoResponse> response_guard(response);
  std::cout << "[res size]: " << response->message().size() << std::endl;
}

/*************** Function test ***************/

void SyncRpcCall(EchoService_Stub& stub) {
  // 32 bytes --> 4M bytes 
  int init_size = 32;
  int epoch_num = 18;
  for (int i = 0; i < epoch_num; i++) {
    EchoRequest request;
    EchoResponse response;
    lightrpc::LightController cntl;
    std::string req_str(init_size, 'A');
    request.set_message(req_str);
    std::cout << "[req size]: " << request.message().size() << std::endl;
    stub.Echo(&cntl, &request, &response, nullptr);
    std::cout << "[res size]: " << response.message().size() << std::endl;
    init_size *= 2;
  }
}

void AsyncRpcCall(EchoService_Stub& stub) {
  // 32 --> 64K bytes 
  int init_size = 32;
  int times = 12;
  for (int i = 0; i < times; i++) {
    EchoRequest request;
    EchoResponse* response = new EchoResponse();
    lightrpc::LightController cntl;
    std::string req_str(init_size, 'A');
    request.set_message(req_str);
    std::cout << "[req size]: " << request.message().size() << std::endl;
    google::protobuf::Closure* done = 
      google::protobuf::NewCallback(&HandleResponse, response);
    stub.Echo(&cntl, &request, response, done);
    init_size *= 2;
  }
  // Wait for the asynchronous call to end.
  std::cout << "Sleep and wait..." << std::endl;
  sleep(5);
}

void SyncMultiThread(EchoService_Stub& stub) {
  int thread_num = 2;
  std::vector<std::thread> threads(thread_num);
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&stub] {
      SyncRpcCall(stub);
    });
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }
}

void AsyncMultiThread(EchoService_Stub& stub) {
  int thread_num = 2;
  std::vector<std::thread> threads(thread_num);
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&stub] {
      AsyncRpcCall(stub);
    });
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }
}


/************** Performance test **************/

/// freq: Number of calls for requests of various sizes.
/// internal: Time interval between rpc requests (ms).

void SyncRpcCall(EchoService_Stub& stub, int freq, int internal) {
  // 8 bytes --> 4M bytes 
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

  std::cout << "req size" << "\t" << "msg size" << std::endl;

  for (int i = 0; i < epoch_num; i++) {
    EchoRequest request;
    EchoResponse response;

    std::string req_str(init_size, 'A');
    request.set_message(req_str);
    msglen[i] = request.ByteSizeLong(); 

    std::cout << req_str.size() << "\t\t" << msglen[i] << std::endl;

    for (int j = 1; j <= freq; j++) {
      lightrpc::LightController cntl;
      clock_gettime(CLOCK_REALTIME, &start);
      stub.Echo(&cntl, &request, &response, nullptr);
      clock_gettime(CLOCK_REALTIME, &end);
      if (response.message().size() != request.message().size()) {
        fprintf(stderr, "not equal");
      }
      time_add(&timers[i], time_diff(start, end));
      if (internal) usleep(internal * 1000);
    }

    init_size *= 2;
    // ns --> us
    avglat[i] = static_cast<float>(time_avg(timers[i], freq) / 1e3);
  }

  FILE *fp = fopen("sync_latency.txt", "w");
  for (int i = 0; i < epoch_num; i++) {
    fprintf(fp, "%u %.2f", msglen[i], avglat[i]);
    if (i != epoch_num - 1) fprintf(fp, "\n");
  }
  fclose(fp);
}

/// Multiple threads share one channel.
void SyncMultiThread(EchoService_Stub& stub, int msg_size, 
                     int threads_num, int freq, int internal) {
  std::vector<std::thread> threads(threads_num);
  std::unordered_map<std::thread::id, 
    timespec> startTime_map_, endTime_map_;
  std::mutex map_mtx_;

  timespec max_time;
  clock_gettime(CLOCK_REALTIME, &max_time);
  
  for (int i = 0; i < threads_num; i++) {
    threads[i] = std::thread([&] {
      EchoRequest request;
      EchoResponse response;
      std::string req_str(msg_size, 'A');
      request.set_message(req_str);
      lightrpc::LightController cntl;

      // System preheating.
      for (int j = 0; j < 100; j++) {
        stub.Echo(&cntl, &request, &response, nullptr);
        if (internal) usleep(internal * 1000);
      }

      timespec start, end;
      clock_gettime(CLOCK_REALTIME, &start);
      for (int j = 0; j < freq; j++) {
        stub.Echo(&cntl, &request, &response, nullptr);
        if (internal) usleep(internal * 1000);
      }
      clock_gettime(CLOCK_REALTIME, &end);
      std::unique_lock<std::mutex> locker(map_mtx_);
      startTime_map_.insert({std::this_thread::get_id(), start});
      endTime_map_.insert({std::this_thread::get_id(), end});
      locker.unlock();
    });
  }

  for (int i = 0; i < threads_num; i++) {
    threads[i].join();
  }

  timespec min_time;
  clock_gettime(CLOCK_REALTIME, &min_time);

  for (auto& kv : startTime_map_) {
    if (time_compare(min_time, kv.second)) {
      min_time = kv.second;
    }
  }

  for (auto& kv : endTime_map_) {
    if (time_compare(kv.second, max_time)) {
      max_time = kv.second;
    }
  }

  float total_time = time_avg(
    time_diff(min_time, max_time), 1) / 1e3;
  uint64_t total_call = threads_num * freq;
  float throughput = total_call * 1e3 / total_time;
  fprintf(stderr, "The total time is %.2f us.\n", total_time);
  fprintf(stderr, "The total call is %ld.\n", total_call);
  fprintf(stderr, "The throughput is %.2f k/s \n", throughput);
}


int main(int argc, char* argv[])
{
  int num_cpus = std::thread::hardware_concurrency();
  std::cout << "Number of CPUs: " << num_cpus << std::endl;
  
  std::string local_ip;
  GetLocalIp(local_ip);

  // int num_threads = std::min(std::max(2, num_cpus / 4), 8);
  lightrpc::ResourceConfig config = {
    .local_ip = local_ip,
    .local_port = -1,
    .num_threads = 1,
    .poll_mode = lightrpc::BUSY_POLLING,
    .block_pool_size = 6 * 1024 * 1024
  };

  std::string server_ip;
  std::cout << "Input the server IP address: ";
  std::cin >> server_ip;

  lightrpc::ClientSharedResource shared_res(config);
  lightrpc::LightChannel channel(server_ip, 1024, &shared_res);
  // lightrpc::LightChannel channel("89.72.32.36", 1024, &shared_res);

  EchoService_Stub stub(&channel);

  /****** Basic tests ******/
  // SyncRpcCall(stub);
  // AsyncRpcCall(stub);
  // SyncMultiThread(stub);
  // AsyncMultiThread(stub);

  /*** Performance Tests ***/
  SyncRpcCall(stub, atoi(argv[1]), atoi(argv[2]));
  // SyncMultiThread(stub, atoi(argv[1]), atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
  return 0;
}