#pragma once

#include "inc/fast_unique.h"
#include "inc/fast_utils.h"

#include <boost/asio.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace fast {

/**
 * This class only supports synchronous RPC calls.
 * Therefore, rpc_id is redundant here.
 */

class FastChannel : public google::protobuf::RpcChannel {
public:
  FastChannel(UniqueResource* unique_rsc, std::string dest_ip, int dest_port);
  virtual ~FastChannel();
  virtual void CallMethod(const google::protobuf::MethodDescriptor* method,
                          google::protobuf::RpcController* controller,
                          const google::protobuf::Message* request,
                          google::protobuf::Message* response,
                          google::protobuf::Closure* done) override;

private:
  UniqueResource* unique_rsc_;
  SafeID safe_id_;
  std::unique_ptr<SafeMRCache> safe_mr_cache_;

  void TryToPollSendWC();
  void ProcessSendWorkCompletion(ibv_wc& send_wc);

  ibv_mr* ProcessNotifyMessage(uint64_t block_addr);
  void ParseAndProcessResponse(void* addr, bool small_msg, 
                               google::protobuf::Message* response);

  static const int default_num_cache_mr;
};

} // namespace fast