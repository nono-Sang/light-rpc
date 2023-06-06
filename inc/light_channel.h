#pragma once

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include "light_global.h"

namespace lightrpc {

struct RpcCallInfo {
  std::promise<bool> &resback;
  google::protobuf::Message *response;
  google::protobuf::Closure *callback;
};

class LightChannel;
class ClientGlobalResource : public GlobalResource {
  friend class LightChannel;

 public:
  ClientGlobalResource(const ResourceConfig &config) : GlobalResource(config) {}
  virtual ~ClientGlobalResource() {}

 private:
  virtual void ProcessRecvWorkCompletion(ibv_wc &wc);
  void ParseResponse(LightChannel *client, void *addr, uint32_t msg_len);

  std::mutex client_mtx_;
  std::unordered_map<uint32_t, LightChannel *> client_map_;
};

class LightChannel : public google::protobuf::RpcChannel {
  friend class ClientGlobalResource;

 public:
  LightChannel(std::string dest_ip, uint16_t dest_port, ClientGlobalResource *global_res);
  virtual ~LightChannel();
  void CallMethod(const google::protobuf::MethodDescriptor *method,
                  google::protobuf::RpcController *controller,
                  const google::protobuf::Message *request,
                  google::protobuf::Message *response,
                  google::protobuf::Closure *done) override;

 private:
  ClientGlobalResource *global_res_;
  rdma_cm_id *conn_id_;

  std::mutex id_mtx_;
  uint32_t rpc_id_;

  // The hashmap from rpc id to call info.
  std::mutex cinfo_mtx_;
  std::unordered_map<uint32_t, RpcCallInfo> cinfo_map_;
};

}  // namespace lightrpc