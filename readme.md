依赖库：
* ibverbs/rdmacm
* boost
* mimalloc
* protobuf

## 使用方式
服务端：
```cpp
lightrpc::ResourceConfig config(local_ip, 1024);
lightrpc::LightServer server(config);
EchoServiceImpl echo_service;
server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
server.BuildAndStart();
```

客户端：
```cpp
lightrpc::ResourceConfig config(local_ip);
lightrpc::ClientGlobalResource global_res(config);
lightrpc::LightChannel channel(server_ip, 1024, &global_res);
EchoService_Stub echo_stub(&channel);
echo_stub.Echo(&cntl, &request, &response, nullptr);   // 同步调用
echo_stub.Echo(&cntl, &request, &response, callback);  // 异步调用
```

`ResourceConfig` 具有如下成员：
```cpp
struct ResourceConfig {
  std::string local_ip;
  int local_port;
  int num_work_threads;
  int num_io_threads;
  PollingMode poll_mode;
  uint32_t block_pool_size;
};
```