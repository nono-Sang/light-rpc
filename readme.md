依赖库：
* ibverbs/rdmacm
* boost
* mimalloc
* protobuf

## 使用方式
服务端：
```cpp
lightrpc::ResourceConfig config(
    local_ip, 1024, num_threads, lightrpc::BUSY_POLLING, 64 * 1024 * 1024);
lightrpc::LightServer server(config);
EchoServiceImpl echo_service;
server.AddService(lightrpc::SERVER_DOESNT_OWN_SERVICE, &echo_service);
server.BuildAndStart();
```

客户端：
```cpp
lightrpc::ResourceConfig config(
    local_ip, -1, num_threads, lightrpc::BUSY_POLLING, , 64 * 1024 * 1024);
lightrpc::ClientGlobalResource global_res(config);
lightrpc::LightChannel channel(server_ip, 1024, &global_res);
EchoService_Stub echo_stub(&channel);
echo_stub.Echo(&cntl, &request, &response, nullptr);   // 同步调用
echo_stub.Echo(&cntl, &request, &response, callback);  // 异步调用
```

代码说明：
* 客户端、服务端进程均存在一份全局资源（包括 RDMA 相关的资源，内存、线程资源等），同一进程中的 QP 会共享该全局资源。用户可以通过 `ResourceConfig` 来配置全局资源，一般情况下，只需要关注以下参数：（1）绑定的本地 IP 和端口号（客户端可忽略）。（2）线程池的线程数量，服务端用于 RPC 请求的处理，客户端用于异步 RPC 的回调处理。（3）轮询模式，`BUSY_POLLING` 会一直占用一个 CPU 核，但效率很高。`EVENT_NOTIFY` 基于事件通知，相对更节省 CPU 资源，但效率低。如果 CPU 核心数较多或者 RPC 请求并发量很大，推荐前者。（4）预注册内存的大小，在一定程度上越大越好。
* 服务端处，直接用配置参数来初始化 server，即 `LightServer server(config)`，然后基于 Protobuf 实现并添加所需的服务，启动 server 即可。
* 客户端处，需要先用配置参数来创建全局资源，即 `ClientGlobalResource global_res(config)`，然后创建 channel 时显示地指定共享的全局资源。这个语义的用处是，在对称环境下，每个节点既作为服务端，也作为客户端，任意两个节点之间都会有连接，我们会让连接不同 server 的 channel 同样共享资源（就像服务端那样）。接着，通过 channel 创建存根以及发起调用。其中 channel 是线程安全的，同步调用会阻塞到 RPC 响应返回，异步调用时，用户需要传入回调来指示响应的后续处理（发送请求交给 RDMA 网卡后，异步调用返回）。