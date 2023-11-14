#include "inc/fast_server.h"
#include "inc/fast_define.h"
#include "inc/fast_log.h"
#include "inc/fast_verbs.h"
#include "build/fast_impl.pb.h"

namespace fast {

const int FastServer::default_num_poll_th = std::max(1U, std::thread::hardware_concurrency() / 8U);

FastServer::FastServer(SharedResource* shared_rsc, int num_poll_th)
  : shared_rsc_(shared_rsc), 
    num_pollers_(num_poll_th), 
    stop_flag_(false) {
  for (int i = 0; i < num_pollers_; ++i) {
    this->poller_pool_.emplace_back([this] {
      this->BusyPollRecvWC();
    });
  }
  this->conn_id_map_ = std::make_unique<SafeHashMap<rdma_cm_id*>>();
  CHECK(rdma_listen(shared_rsc_->GetConnMgrID(), listen_backlog) == 0);
  LOG_INFO("Start listening on port %d", shared_rsc_->GetLocalPort());
}

FastServer::~FastServer() {
  for (auto &kv : service_map_) {
    if (kv.second.ownership == SERVER_OWNS_SERVICE) {
      delete kv.second.service;
    }
  }
  stop_flag_ = true;
  for (auto& poll_th : poller_pool_) {
    poll_th.join();
  }
}

void FastServer::AddService(ServiceOwnership ownership, google::protobuf::Service* service) {
  std::string name = service->GetDescriptor()->name();
  CHECK(service_map_.find(name) == service_map_.end());
  ServiceInfo service_info;
  service_info.ownership = ownership;
  service_info.service = service;
  service_map_.emplace(name, service_info);
}

void FastServer::BuildAndStart() {
  rdma_cm_event_type ent_type;
  rdma_cm_id* conn_id = nullptr;
  rdma_cm_event* cm_ent = nullptr;
  auto listen_id = shared_rsc_->GetConnMgrID();
  while (true) {
    CHECK(rdma_get_cm_event(listen_id->channel, &cm_ent) == 0);
    ent_type = cm_ent->event;
    conn_id = cm_ent->id;
    if (ent_type == RDMA_CM_EVENT_CONNECT_REQUEST) {
      CHECK(rdma_ack_cm_event(cm_ent) == 0);
      CHECK(conn_id->verbs == listen_id->verbs);
      CHECK(conn_id->pd == listen_id->pd);
      ProcessConnectRequest(conn_id);
    } else if (ent_type == RDMA_CM_EVENT_ESTABLISHED) {
      CHECK(rdma_ack_cm_event(cm_ent) == 0);
      // ProcessConnectEstablish(conn_id);
    } else if (ent_type == RDMA_CM_EVENT_DISCONNECTED) {
      CHECK(rdma_ack_cm_event(cm_ent) == 0);
      ProcessDisconnect(conn_id);
    } else {
      LOG_ERR("Other event value: %d.", ent_type);
    }
  }
}

void FastServer::ProcessConnectRequest(rdma_cm_id* conn_id) {
  // Create and record queue pair.
  shared_rsc_->CreateNewQueuePair(conn_id);
  conn_id_map_->SafeInsert(conn_id->qp->qp_num, conn_id);

  // Accept the connection request.
  rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.rnr_retry_count = 7;  // infinite retry
  CHECK(rdma_accept(conn_id, &cm_params) == 0);
}

void FastServer::ProcessDisconnect(rdma_cm_id* conn_id) {
  conn_id_map_->SafeErase(conn_id->qp->qp_num);
  // https://blog.lucode.net/RDMA/rdma-cm-introduction-and-create-qp-with-external-cq.html
  rdma_destroy_qp(conn_id);
  CHECK(rdma_destroy_id(conn_id) == 0);
}

void FastServer::BusyPollRecvWC() {
  auto recv_cq = shared_rsc_->GetConnMgrID()->recv_cq;
  ibv_wc recv_wc;
  memset(&recv_wc, 0, sizeof(recv_wc));
  while (true) {
    int num = ibv_poll_cq(recv_cq, 1, &recv_wc);
    CHECK(num != -1);
    if (num == 1) {
      CHECK(recv_wc.status == IBV_WC_SUCCESS);
      ProcessRecvWorkCompletion(recv_wc);
    } else {
      // Give up the CPU.
      std::this_thread::yield();
    }
  }
}

void FastServer::ProcessRecvWorkCompletion(ibv_wc& recv_wc) {
  uint32_t imm_data = ntohl(recv_wc.imm_data);
  ibv_wc_opcode opcode = recv_wc.opcode;
  rdma_cm_id* conn_id = conn_id_map_->SafeGet(recv_wc.qp_num);
  uint64_t recv_addr = recv_wc.wr_id;

  boost::asio::io_context* io_ctx = shared_rsc_->GetIOContext();

  /// NOTE: Post one recv WR for receiving next request.
  uint64_t block_addr = 0;
  shared_rsc_->GetOneBlockFromGlobalPool(block_addr);
  shared_rsc_->PostOneRecvRequest(block_addr);

  if (imm_data == FAST_SmallMessage) {
    char* msg_buf = reinterpret_cast<char*>(recv_addr);
    io_ctx->post([this, conn_id, msg_buf] {
      this->ParseAndProcessRequest(conn_id, msg_buf, true);
    });
  } else {
    CHECK(imm_data == FAST_NotifyMessage);
    io_ctx->post([this, conn_id, recv_addr] {
      this->ProcessNotifyMessage(conn_id, recv_addr);
    });
  }
}

void FastServer::TryToPollSendWC(rdma_cm_id* conn_id) {
  ibv_wc send_wc;
  memset(&send_wc, 0, sizeof(send_wc));
  while (true) {
    int num = ibv_poll_cq(conn_id->send_cq, 1, &send_wc);
    CHECK(num != -1);
    if (num == 0) break;
    CHECK(send_wc.status == IBV_WC_SUCCESS);
    ProcessSendWorkCompletion(send_wc);
    memset(&send_wc, 0, sizeof(send_wc));
  }
}

void FastServer::ProcessSendWorkCompletion(ibv_wc& send_wc) {
  if (send_wc.wr_id == 0) return;
  int th_idx = shared_rsc_->GetThreadIndex(std::this_thread::get_id());
  if (send_wc.opcode == IBV_WC_RDMA_WRITE) {
    ibv_mr* large_mr = reinterpret_cast<ibv_mr*>(send_wc.wr_id);
    shared_rsc_->PutOneMRIntoCache(th_idx, large_mr);
  } else {
    // The opcode is IBV_WC_SEND.
    shared_rsc_->PutOneBlockIntoLocalCache(th_idx, send_wc.wr_id);
  }
}

void FastServer::ProcessNotifyMessage(rdma_cm_id* conn_id, uint64_t block_addr) {
  int th_idx = shared_rsc_->GetThreadIndex(std::this_thread::get_id());
  // Step-1: Get the notify message.
  char* block_buf = reinterpret_cast<char*>(block_addr);
  NotifyMessage notify_msg;
  CHECK(notify_msg.ParseFromArray(block_buf, fixed_noti_bytes));
  // Return the block which stores notify message.
  shared_rsc_->PutOneBlockIntoLocalCache(th_idx, block_addr);
  uint32_t total_length = notify_msg.total_len();

  // Step-2: Prepare a large MR.
  ibv_mr* large_recv_mr = shared_rsc_->GetOneMRFromCache(th_idx, total_length + 1);
  if (large_recv_mr == nullptr) {
    large_recv_mr = shared_rsc_->AllocAndRegisterMR(total_length + 1);
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
  WriteInlineMessage(conn_id->qp, 
                     auth_addr, 
                     fixed_auth_bytes + 1, 
                     notify_msg.remote_key(), 
                     notify_msg.remote_addr());
  
  /// NOTE: Try to poll send CQEs.
  this->TryToPollSendWC(conn_id);

  // Step-3: Get the large message (busy check).
  volatile char* flag = msg_buf + total_length;
  while (*flag != '1') {}

  this->ParseAndProcessRequest(conn_id, large_recv_mr, false);
}

void FastServer::ParseAndProcessRequest(rdma_cm_id* conn_id, void* addr, bool small_msg) {
  int th_idx = shared_rsc_->GetThreadIndex(std::this_thread::get_id());
  char* msg_buf = nullptr;
  if (small_msg) {
    msg_buf = reinterpret_cast<char*>(addr);
  } else {
    auto large_mr = reinterpret_cast<ibv_mr*>(addr);
    msg_buf = reinterpret_cast<char*>(large_mr->addr);
  }

  TotalLengthOfMsg part1;
  part1.ParseFromArray(msg_buf, fixed32_bytes);
  uint32_t total_length = part1.total_len();

  LengthOfMetaData part2;
  part2.ParseFromArray(msg_buf + fixed32_bytes, fixed32_bytes);
  uint32_t metadata_length = part2.metadata_len();

  MetaDataOfRequest part3;
  part3.ParseFromArray(msg_buf + 2 * fixed32_bytes, metadata_length);
  uint32_t rpc_id = part3.rpc_id();
  std::string service_name = part3.service_name();
  std::string method_name = part3.method_name();

  auto iter = service_map_.find(service_name);
  auto service = iter->second.service;
  auto method = service->GetDescriptor()->FindMethodByName(method_name);

  // Create request and response instance.
  auto request = service->GetRequestPrototype(method).New();
  auto response = service->GetResponsePrototype(method).New();

  uint32_t payload_length = total_length - 2 * fixed32_bytes - metadata_length;
  CHECK(request->ParseFromArray(
    msg_buf + 2 * fixed32_bytes + metadata_length, payload_length));
  
  /// NOTE: Return the occupied resources.
  if (small_msg) {
    uint64_t block_addr = reinterpret_cast<uint64_t>(addr);
    shared_rsc_->PutOneBlockIntoLocalCache(th_idx, block_addr);
  } else {
    shared_rsc_->PutOneMRIntoCache(th_idx, reinterpret_cast<ibv_mr*>(addr));
  }

  CallBackArgs args;
  args.rpc_id = rpc_id;
  args.conn_id = conn_id;
  args.request = request;
  args.response = response;

  auto done = google::protobuf::NewCallback(this, &FastServer::ReturnRPCResponse, args);
  service->CallMethod(method, nullptr, request, response, done);
}

void FastServer::ReturnRPCResponse(CallBackArgs args) {
  int th_idx = shared_rsc_->GetThreadIndex(std::this_thread::get_id());
  // Use RAII mechanism to release resources.
  std::unique_ptr<google::protobuf::Message> request_guard(args.request);
  std::unique_ptr<google::protobuf::Message> response_guard(args.response);

  // message layout: |1.header|2.payload|
  uint32_t part2_len = args.response->ByteSizeLong();
  uint32_t total_length = fixed_rep_head_bytes + part2_len;

  ResponseHead part1;
  part1.set_rpc_id(args.rpc_id);
  part1.set_total_len(total_length);
  CHECK(part1.ByteSizeLong() == fixed_rep_head_bytes);

  // Define the serialization function.
  auto serialize_func = [&part1, &args, part2_len] (char* msg_buf) {
    CHECK(part1.SerializeToArray(msg_buf, fixed_rep_head_bytes));
    CHECK(args.response->SerializeToArray(msg_buf + fixed_rep_head_bytes, part2_len));
  };

  uint32_t local_key = shared_rsc_->GetLocalKey();
  uint32_t remote_key = shared_rsc_->GetRemoteKey();

  if (total_length <= max_inline_data) {
    char msg_buf[max_inline_data];
    uint64_t msg_addr = reinterpret_cast<uint64_t>(msg_buf);
    serialize_func(msg_buf);
    SendInlineMessage(args.conn_id->qp, 
                      FAST_SmallMessage, 
                      msg_addr, 
                      total_length);
  } else if (total_length <= msg_threshold) {
    uint64_t msg_addr = 0;
    shared_rsc_->GetOneBlockFromLocalCache(th_idx, msg_addr);
    serialize_func(reinterpret_cast<char*>(msg_addr));
    SendSmallMessage(args.conn_id->qp, 
                     FAST_SmallMessage, 
                     msg_addr, 
                     total_length, 
                     shared_rsc_->GetLocalKey());
  } else {
    // Obtain one block for receiving authority message.
    uint64_t auth_addr = 0;
    shared_rsc_->GetOneBlockFromLocalCache(th_idx, auth_addr);
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
    SendInlineMessage(args.conn_id->qp, 
                      FAST_NotifyMessage, 
                      noti_addr, 
                      fixed_noti_bytes);
    
    // Step-2: Prepare a large MR and perform serialization.
    ibv_mr* large_send_mr = shared_rsc_->GetOneMRFromCache(th_idx, total_length + 1);
    if (large_send_mr == nullptr) {
      large_send_mr = shared_rsc_->AllocAndRegisterMR(total_length + 1);
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
    WriteLargeMessage(args.conn_id->qp,
                      large_send_mr, 
                      total_length + 1, 
                      authority_msg.remote_key(), 
                      authority_msg.remote_addr());
    
    // Return the block which stores authority message
    shared_rsc_->PutOneBlockIntoLocalCache(th_idx, auth_addr);
  }

  /// NOTE: Try to poll send CQEs.
  this->TryToPollSendWC(args.conn_id);
}

} // namespace fast