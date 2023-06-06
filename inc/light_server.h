#pragma once

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include "light_global.h"

namespace lightrpc {

enum ServiceOwnership { SERVER_OWNS_SERVICE, SERVER_DOESNT_OWN_SERVICE };

struct ServiceInfo {
  ServiceOwnership ownership;
  google::protobuf::Service *service;
};

struct CallBackArgs {
  uint32_t rpc_id;
  ibv_qp *conn_qp;
  google::protobuf::Message *request;
  google::protobuf::Message *response;
};

class LightServer : public GlobalResource {
 public:
  LightServer(ResourceConfig config);
  virtual ~LightServer();
  void AddService(ServiceOwnership ownership, google::protobuf::Service *service);
  void BuildAndStart();

 private:
  void ProcessConnectRequest(rdma_cm_id *conn_id);
  void ProcessConnectEstablish(rdma_cm_id *conn_id);
  void ProcessDisconnect(rdma_cm_id *conn_id);

  virtual void ProcessRecvWorkCompletion(ibv_wc &wc);
  void ParseRequest(ibv_qp *conn_qp, void *addr, uint32_t msg_len);
  void ReturnResponse(CallBackArgs args);

  // The hashmap from name to service.
  std::unordered_map<std::string, ServiceInfo> service_map_;
  // The hashmap from qp number to ibv_qp*.
  std::mutex qp_num_mtx_;
  std::unordered_map<uint32_t, ibv_qp *> qp_num_map_;
};

}  // namespace lightrpc