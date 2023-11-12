#pragma once

#include "inc/fast_shared.h"
#include "inc/fast_utils.h"

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace fast {

enum ServiceOwnership { 
  SERVER_OWNS_SERVICE, 
  SERVER_DOESNT_OWN_SERVICE 
};

struct ServiceInfo {
  ServiceOwnership ownership;
  google::protobuf::Service* service;
};

struct CallBackArgs {
  uint32_t rpc_id;
  rdma_cm_id* conn_id;
  google::protobuf::Message* request;
  google::protobuf::Message* response;
  CallBackArgs() = default;
  CallBackArgs(const CallBackArgs&) = default;
};

class FastServer {
public:
  FastServer(SharedResource* shared_rsc, 
             int num_poll_th = default_num_poll_th, 
             int num_work_th = default_num_work_th);
  ~FastServer();
  void AddService(ServiceOwnership ownership, google::protobuf::Service* service);
  void BuildAndStart();

private:
  SharedResource* shared_rsc_; 

  int num_pollers_;
  volatile std::atomic<bool> stop_flag_;
  std::vector<std::thread> poller_pool_;
  IOContextPool io_ctx_pool_;

  std::unique_ptr<SafeMRCache> safe_mr_cache_;
  std::unique_ptr<SafeHashMap<rdma_cm_id*>> conn_id_map_;  // key is qp number
  std::unordered_map<std::string, ServiceInfo> service_map_;  // key is service name

  /* Private functions */
  void ProcessConnectRequest(rdma_cm_id* conn_id);
  void ProcessDisconnect(rdma_cm_id* conn_id);
  void BusyPollRecvWC();
  void ProcessRecvWorkCompletion(ibv_wc& recv_wc);
  void TryToPollSendWC(rdma_cm_id* conn_id);
  void ProcessSendWorkCompletion(ibv_wc& send_wc);
  void ProcessNotifyMessage(rdma_cm_id* conn_id, uint64_t block_addr);
  void ParseAndProcessRequest(rdma_cm_id* conn_id, void* addr, bool small_msg);
  void ReturnRPCResponse(CallBackArgs args);

  static const int default_num_poll_th;
  static const int default_num_work_th;
  static const int default_num_cache_mr;
};
  
} // namespace fast