#include "inc/fast_channel.h"
#include "inc/fast_define.h"
#include "inc/fast_log.h"
#include "inc/fast_verbs.h"
#include "build/fast_impl.pb.h"

static const int timeout_in_ms = 1000;  // ms

namespace fast {

const int FastChannel::default_num_cache_mr = 10;

FastChannel::FastChannel(UniqueResource* unique_rsc, std::string dest_ip, int dest_port)
  : unique_rsc_(unique_rsc), safe_id_(1) {
  this->safe_mr_cache_ = std::make_unique<SafeMRCache>(default_num_cache_mr);

  // Resolve remote address.
  sockaddr_in dest_addr;
  memset(&dest_addr, 0, sizeof(dest_addr));
  dest_addr.sin_family = AF_INET;
  dest_addr.sin_addr.s_addr = inet_addr(dest_ip.c_str());
  dest_addr.sin_port = htons(dest_port);

  auto cm_id = unique_rsc_->GetConnMgrID();

  CHECK(rdma_resolve_addr(cm_id, nullptr, reinterpret_cast<sockaddr*>(&dest_addr), timeout_in_ms) == 0);
  rdma_cm_event* cm_ent = nullptr;
  CHECK(rdma_get_cm_event(cm_id->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ADDR_RESOLVED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  // Resolve an RDMA route to the destination address.
  CHECK(rdma_resolve_route(cm_id, timeout_in_ms) == 0);
  CHECK(rdma_get_cm_event(cm_id->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  // Connect to remote address.
  rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.rnr_retry_count = 7;  // infinite retry
  CHECK(rdma_connect(cm_id, &cm_params) == 0);
  CHECK(rdma_get_cm_event(cm_id->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ESTABLISHED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);
}

FastChannel::~FastChannel() {
  auto cm_id = unique_rsc_->GetConnMgrID();

  CHECK(rdma_disconnect(cm_id) == 0);
  rdma_cm_event* cm_ent = nullptr;
  CHECK(rdma_get_cm_event(cm_id->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_DISCONNECTED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);
}

void FastChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                             google::protobuf::RpcController* controller,
                             const google::protobuf::Message* request,
                             google::protobuf::Message* response,
                             google::protobuf::Closure* done) {
  uint32_t rpc_id = safe_id_.GetAndIncOne();

  // message layout: |1.total length|2.metadata length|3.metadata|4.payload|
  uint32_t part4_len = request->ByteSizeLong();

  MetaDataOfRequest part3;
  part3.set_rpc_id(rpc_id);
  part3.set_service_name(method->service()->name());
  part3.set_method_name(method->name());
  uint32_t part3_len = part3.ByteSizeLong();

  LengthOfMetaData part2;
  part2.set_metadata_len(part3_len);
  CHECK(part2.ByteSizeLong() == fixed32_bytes);

  uint32_t total_length = 2 * fixed32_bytes + part3_len + part4_len;
  TotalLengthOfMsg part1;
  part1.set_total_len(total_length);
  CHECK(part1.ByteSizeLong() == fixed32_bytes);

  // Define the serialization function.
  auto serialize_func = [&part1, &part2, &part3, &request, part3_len, part4_len](char* msg_buf) {
    CHECK(part1.SerializeToArray(msg_buf, fixed32_bytes));
    CHECK(part2.SerializeToArray(msg_buf + fixed32_bytes, fixed32_bytes));
    CHECK(part3.SerializeToArray(msg_buf + 2 * fixed32_bytes, part3_len));
    CHECK(request->SerializeToArray(msg_buf + 2 * fixed32_bytes + part3_len, part4_len));
  };

  /// NOTE: Post one recv WR for receiving response.
  uint64_t block_addr = 0;
  unique_rsc_->ObtainOneBlock(block_addr);
  unique_rsc_->PostOneRecvRequest(block_addr);

  ibv_qp* local_qp = unique_rsc_->GetQueuePair();
  uint32_t local_key = unique_rsc_->GetLocalKey();
  uint32_t remote_key = unique_rsc_->GetRemoteKey();

  if (total_length <= max_inline_data) {
    char msg_buf[max_inline_data];
    uint64_t msg_addr = reinterpret_cast<uint64_t>(msg_buf);
    serialize_func(msg_buf);
    SendInlineMessage(local_qp, 
                      FAST_SmallMessage, 
                      msg_addr, 
                      total_length);
  } else if (total_length <= msg_threshold) {
    uint64_t msg_addr = 0;
    unique_rsc_->ObtainOneBlock(msg_addr);
    serialize_func(reinterpret_cast<char*>(msg_addr));
    SendSmallMessage(local_qp, 
                     FAST_SmallMessage, 
                     msg_addr, 
                     total_length, 
                     unique_rsc_->GetLocalKey());
  } else {
    // Obtain one block for receiving authority message.
    uint64_t auth_addr = 0;
    unique_rsc_->ObtainOneBlock(auth_addr);
    char* auth_buf = reinterpret_cast<char*>(auth_addr);
    // [Receiver]: set the flag byte to '0'.
    memset(auth_buf + fixed_auth_bytes, '0', 1);

    // Step-1: Create and send the notify message (send op).
    NotifyMessage notify_msg;
    notify_msg.set_total_len(total_length);
    notify_msg.set_remote_key(remote_key);
    notify_msg.set_remote_addr(auth_addr);
    CHECK(notify_msg.ByteSizeLong() == fixed_noti_bytes);
    char noti_buf[max_inline_data];
    uint64_t noti_addr = reinterpret_cast<uint64_t>(noti_buf);
    CHECK(notify_msg.SerializeToArray(noti_buf, fixed_noti_bytes));
    SendInlineMessage(local_qp, 
                      FAST_NotifyMessage, 
                      noti_addr, 
                      fixed_noti_bytes);
    
    // Step-2: Prepare a large MR and perform serialization.
    ibv_mr* large_send_mr = safe_mr_cache_->SafeGetAndEraseMR(total_length + 1);
    if (large_send_mr == nullptr) {
      large_send_mr = unique_rsc_->AllocAndRegisterMR(total_length + 1);
    }
    char* msg_buf = reinterpret_cast<char*>(large_send_mr->addr);
    serialize_func(msg_buf);
    // [Sender]: set the flag byte to '1'.
    memset(msg_buf + total_length, '1', 1);

    // Step-3: Get the authority message (busy check).
    volatile char* flag = auth_buf + fixed_auth_bytes;
    while (*flag != '1') {}
    AuthorityMessage authority_msg;
    CHECK(authority_msg.ParseFromArray(auth_buf, fixed_auth_bytes));
    
    // Step-4: Send the large message (write op).
    WriteLargeMessage(local_qp,
                      large_send_mr, 
                      total_length + 1, 
                      authority_msg.remote_key(), 
                      authority_msg.remote_addr());
    
    // Return the block which stores authority message
    unique_rsc_->ReturnOneBlock(auth_addr);
  }

  /// NOTE: Try to poll send CQEs.
  this->TryToPollSendWC();

  auto recv_cq = unique_rsc_->GetRecvCQ();
  ibv_wc recv_wc;
  memset(&recv_wc, 0, sizeof(recv_wc));
  while (true) {
    int num = ibv_poll_cq(recv_cq, 1, &recv_wc);
    CHECK(num != -1);
    if (num == 1) break;  // Just get one WC.
  }

  CHECK(recv_wc.status == IBV_WC_SUCCESS);
  uint32_t imm_data = ntohl(recv_wc.imm_data);
  uint64_t recv_addr = recv_wc.wr_id;

  if (imm_data == FAST_SmallMessage) {
    char* msg_buf = reinterpret_cast<char*>(recv_addr);
    ParseAndProcessResponse(msg_buf, true, response);
  } else {
    CHECK(imm_data == FAST_NotifyMessage);
    auto large_recv_mr = ProcessNotifyMessage(recv_addr);
    ParseAndProcessResponse(large_recv_mr, false, response);
  }
}

void FastChannel::TryToPollSendWC() {
  ibv_cq* send_cq = unique_rsc_->GetSendCQ();
  ibv_wc send_wc;
  memset(&send_wc, 0, sizeof(send_wc));
  while (true) {
    int num = ibv_poll_cq(send_cq, 1, &send_wc);
    CHECK(num != -1);
    if (num == 0) break;
    CHECK(send_wc.status == IBV_WC_SUCCESS);
    ProcessSendWorkCompletion(send_wc);
    memset(&send_wc, 0, sizeof(send_wc));
  }
}

void FastChannel::ProcessSendWorkCompletion(ibv_wc& send_wc) {
  if (send_wc.opcode == IBV_WC_RDMA_WRITE && send_wc.wr_id) {
    ibv_mr* large_mr = reinterpret_cast<ibv_mr*>(send_wc.wr_id);
    safe_mr_cache_->SafePutMR(large_mr);
  } else {
    // The opcode is IBV_WC_SEND.
    if (send_wc.wr_id) unique_rsc_->ReturnOneBlock(send_wc.wr_id);
  }
}

ibv_mr* FastChannel::ProcessNotifyMessage(uint64_t block_addr) {
  ibv_qp* local_qp = unique_rsc_->GetQueuePair();

  // Step-1: Get the notify message.
  char* block_buf = reinterpret_cast<char*>(block_addr);
  NotifyMessage notify_msg;
  CHECK(notify_msg.ParseFromArray(block_buf, fixed_noti_bytes));
  // Return the block which stores notify message.
  unique_rsc_->ReturnOneBlock(block_addr);
  uint32_t total_length = notify_msg.total_len();

  // Step-2: Prepare a large MR.
  ibv_mr* large_recv_mr = safe_mr_cache_->SafeGetAndEraseMR(total_length + 1);
  if (large_recv_mr == nullptr) {
    large_recv_mr = unique_rsc_->AllocAndRegisterMR(total_length + 1);
  }
  char* msg_buf = reinterpret_cast<char*>(large_recv_mr->addr);
  // [Receiver]: set the flag byte to '0'.
  memset(msg_buf + total_length, '0', 1);

  // Step-3: Create and send the authority message (write op).
  AuthorityMessage authority_msg;
  authority_msg.set_remote_key(large_recv_mr->rkey);
  authority_msg.set_remote_addr(reinterpret_cast<uint64_t>(large_recv_mr->addr));
  CHECK(authority_msg.ByteSizeLong() == fixed_auth_bytes);
  char auth_buf[max_inline_data];
  uint64_t auth_addr = reinterpret_cast<uint64_t>(auth_buf);
  CHECK(authority_msg.SerializeToArray(auth_buf, fixed_auth_bytes));
  // [Sender]: set the flag byte to '1'.
  memset(auth_buf + fixed_auth_bytes, '1', 1);
  WriteInlineMessage(local_qp, 
                     auth_addr, 
                     fixed_auth_bytes + 1, 
                     notify_msg.remote_key(), 
                     notify_msg.remote_addr());
  
  /// NOTE: Try to poll send CQEs.
  this->TryToPollSendWC();

  // Step-3: Get the large message (busy check).
  volatile char* flag = msg_buf + total_length;
  while (*flag != '1') {}

  return large_recv_mr;
}

void FastChannel::ParseAndProcessResponse(void* addr, 
                                          bool small_msg, 
                                          google::protobuf::Message* response) {
  char* msg_buf = nullptr;
  if (small_msg) {
    msg_buf = reinterpret_cast<char*>(addr);
  } else {
    auto large_mr = reinterpret_cast<ibv_mr*>(addr);
    msg_buf = reinterpret_cast<char*>(large_mr->addr);
  }

  ResponseHead part1;
  part1.ParseFromArray(msg_buf, fixed_rep_head_bytes);
  uint32_t rpc_id = part1.rpc_id();
  uint32_t total_length = part1.total_len();

  uint32_t payload_length = total_length - fixed_rep_head_bytes;
  response->ParseFromArray(msg_buf + fixed_rep_head_bytes, payload_length);

  /// NOTE: Return the occupied resources.
  if (small_msg) {
    uint64_t block_addr = reinterpret_cast<uint64_t>(addr);
    unique_rsc_->ReturnOneBlock(block_addr);
  } else {
    safe_mr_cache_->SafePutMR(reinterpret_cast<ibv_mr*>(addr));
  }
}
  
} // namespace fast