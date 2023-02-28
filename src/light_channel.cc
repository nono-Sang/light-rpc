#include "src/light_channel.h"
#include "src/light_api.h"
#include "proto/light_impl.pb.h"

#include <mimalloc/mimalloc.h>

namespace lightrpc {

void ClientSharedResource::ProcessRecvWorkCompletion(ibv_wc& wc) {
  uint32_t qp_num = wc.qp_num;
  uint64_t recv_addr = wc.wr_id;
  uint32_t imm_data = ntohl(wc.imm_data);
  ibv_wc_opcode opcode = wc.opcode;
  service_pool_[round_robin_idx_]->post(
    [this, qp_num, recv_addr, imm_data, opcode] {
    ReadLock locker(shared_mtx_);
    auto it = client_map_.find(qp_num);
    CHECK(it != client_map_.end());
    LightChannel* client = it->second;
    locker.unlock();

    ibv_qp* conn_qp = client->conn_id_->qp;

    // The imm_data is rpc_id.
    if (opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
      ReturnOneBlock(recv_addr);
      ibv_mr* msg_mr = GetMemoryRegion(imm_data, conn_qp);
      ParseAndRunAfter(client, msg_mr, 
        static_cast<uint32_t>(msg_mr->length));
      return;
    }

    // The imm_data is message length.
    CHECK(opcode == IBV_WC_RECV);
    if (imm_data <= msg_threshold) {
      char* msg_addr = reinterpret_cast<char*>(recv_addr);
      ParseAndRunAfter(client, msg_addr, imm_data);
    } else {
      ibv_mr* msg_mr = AllocLargeMsgMR(imm_data, conn_qp);
      ProcessLargeMessage(
        conn_qp, msg_mr, recv_addr, imm_data, CLIENT_FLAG);
      if (imm_data >= max_threshold) return;
      else ParseAndRunAfter(client, msg_mr, imm_data);
    }
  });
  round_robin_idx_ = (round_robin_idx_ + 1) % config_.num_threads;
}

void ClientSharedResource::ParseAndRunAfter(LightChannel* client,
                                            void* addr,
                                            uint32_t msg_len) {
  char* msg_addr = nullptr;
  if (msg_len <= msg_threshold) {
    msg_addr = reinterpret_cast<char*>(addr);
  } else {
    msg_addr = reinterpret_cast<char*>(
      reinterpret_cast<ibv_mr*>(addr)->addr);
  }

  uint32_t rpc_id = 0;
  memcpy(&rpc_id, msg_addr, res_head_len);

  // Get the corresponding callinfo.
  std::unique_lock<std::mutex> find_locker(client->map_mtx_);
  auto iter = client->callinfo_map_.find(rpc_id);
  CHECK(iter != client->callinfo_map_.end());
  RpcCallInfo& call_info = iter->second;
  find_locker.unlock();

  // Assign value to response.
  uint32_t response_size = msg_len - res_head_len;
  CHECK(call_info.response->ParseFromArray(
    msg_addr + res_head_len, response_size));
  
  if (msg_len <= msg_threshold) 
    ReturnOneBlock(reinterpret_cast<uint64_t>(msg_addr));
  if (msg_len >= max_threshold) {
    CHECK(ibv_dereg_mr(
      reinterpret_cast<ibv_mr*>(addr)) == 0);
    free(msg_addr);
  }
  
  // For synchronous rpc, notify the user thread. 
  // For asynchronous rpc, run the callback.
  if (call_info.callback) call_info.callback->Run();
  else call_info.prom.set_value(true);

  std::unique_lock<std::mutex> erase_locker(client->map_mtx_);
  client->callinfo_map_.erase(rpc_id);
  erase_locker.unlock();
}

LightChannel::LightChannel(std::string dest_ip, 
                           uint16_t dest_port, 
                           ClientSharedResource* shared_res)
  : shared_res_(shared_res), rpc_id_(0) {
  // Create event channel and cm id.
  ent_chan_ = rdma_create_event_channel();
  CHECK(ent_chan_ != nullptr);
  CHECK(rdma_create_id(
    ent_chan_, &conn_id_, nullptr, RDMA_PS_TCP) == 0);
  
  // Resolve remote address and bind local device.
  sockaddr_in dest_addr;
  memset(&dest_addr, 0, sizeof(dest_addr));
  dest_addr.sin_family = AF_INET;
  dest_addr.sin_addr.s_addr = inet_addr(dest_ip.c_str());
  dest_addr.sin_port = htons(dest_port);

  sockaddr_in local_addr;
  memset(&local_addr, 0, sizeof(local_addr));
  local_addr.sin_family = AF_INET;
  local_addr.sin_addr.s_addr = inet_addr(
    shared_res_->config_.local_ip.c_str());
  
  CHECK(rdma_resolve_addr(conn_id_, 
    reinterpret_cast<sockaddr*>(&local_addr), 
    reinterpret_cast<sockaddr*>(&dest_addr), 
    timeout_in_ms) == 0);
  
  rdma_cm_event* cm_ent = nullptr;
  CHECK(rdma_get_cm_event(ent_chan_, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ADDR_RESOLVED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  CHECK(conn_id_->verbs == shared_res_->cm_id_->verbs);
  CHECK(conn_id_->pd == shared_res_->cm_id_->pd);

  shared_res_->CreateQueuePair(conn_id_);
  WriteLock locker(shared_res_->shared_mtx_);
  shared_res_->client_map_.insert({conn_id_->qp->qp_num, this});
  locker.unlock();

  // Resolve an RDMA route to the destination address.
  CHECK(rdma_resolve_route(conn_id_, timeout_in_ms) == 0);
  CHECK(rdma_get_cm_event(ent_chan_, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  // Connect to remote address.
  rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  // cm_params.rnr_retry_count = 7;  // infinite retry
  CHECK(rdma_connect(conn_id_, &cm_params) == 0);
  CHECK(rdma_get_cm_event(ent_chan_, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ESTABLISHED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  LOG_INFO("Connection to <IP: %s, Port: %hu> established.", 
    dest_ip.c_str(), dest_port);
}

void LightChannel::CallMethod(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    google::protobuf::Closure* done) {
  CHECK(request != nullptr);
  // Set metadata for the request.
  uint32_t corr_id = 0;
  std::unique_lock<std::mutex> id_locker(id_mtx_);
  corr_id = ++rpc_id_;
  id_locker.unlock();

  RequestMetadata metadata;
  metadata.set_rpc_id(corr_id);
  metadata.set_service_name(method->service()->name());
  metadata.set_method_name(method->name());

  uint32_t request_size = request->ByteSizeLong();
  uint32_t metadata_size = metadata.ByteSizeLong();
  CHECK(metadata_size <= max_metadata_size);
  uint32_t msg_len = req_fixed_len + metadata_size + request_size;

  // Define the serialization lambda function. As for large message,
  // use a tag byte to mark the end of the client's write operation.
  char* msg_buf = nullptr;
  auto SerializeFunc = [&]() {
    memcpy(msg_buf, &metadata_size, req_fixed_len);
    CHECK(metadata.SerializeToArray(
      msg_buf + req_fixed_len, metadata_size));
    CHECK(request->SerializeToArray(
      msg_buf + req_fixed_len + metadata_size, request_size));
    if (msg_len > msg_threshold && msg_len < max_threshold)
      memset(msg_buf + msg_len, '1', 1);
  };

  // Record the CallInfo in the HashMap.
  std::promise<bool> corr_prom;
  auto corr_future = corr_prom.get_future();
  RpcCallInfo corr_info = {
    .prom = corr_prom, .response = response, .callback = done };
  std::unique_lock<std::mutex> map_locker(map_mtx_);
  callinfo_map_.insert({corr_id, corr_info});
  map_locker.unlock();

  // Post a recv-req in advance for receiving response.
  uint64_t recv_addr = 0;
  shared_res_->ObtainOneBlock(recv_addr);
  shared_res_->PostOneRecvRequest(recv_addr);
  // For small msg, client will copy it to the block.
  // For large msg, server will write mr info to the block.
  uint64_t block_addr = 0;
  shared_res_->ObtainOneBlock(block_addr);

  uint32_t local_key = shared_res_->shared_mr_->lkey;
  uint32_t remote_key = shared_res_->shared_mr_->rkey;

  if (msg_len <= msg_threshold) {
    msg_buf = reinterpret_cast<char*>(
      mi_malloc(msg_len));
    CHECK(msg_buf != nullptr);
    SerializeFunc(); // serialize request
    memcpy(reinterpret_cast<char*>(
      block_addr), msg_buf, msg_len);
    mi_free(msg_buf);
    SendSmallMessage(
      conn_id_->qp, block_addr, msg_len, local_key);
  } else {
    // Send the msg length and remote info.
    char* ptr_tag = reinterpret_cast<char*>(
      block_addr) + sizeof(RemoteInfo);
    memset(ptr_tag, '0', 1);

    if (msg_len >= max_threshold) {
      RemoteInfoWithId info = {
        .rpc_id = corr_id,
        .remote_key = remote_key,
        .remote_addr = block_addr
      };
      SendNotifyMessage(conn_id_->qp, msg_len, info);
    } else {
      RemoteInfo info = {
        .remote_key = remote_key,
        .remote_addr = block_addr
      };
      SendNotifyMessage(conn_id_->qp, msg_len, info);
    }
    
    // Allocate memory, serialize and register.
    uint32_t alloc_len = 0;
    if (msg_len >= max_threshold) alloc_len = msg_len;
    else alloc_len = msg_len + 1;
    msg_buf = reinterpret_cast<char*>(
      mi_malloc(alloc_len));
    CHECK(msg_buf != nullptr);
    SerializeFunc(); // serialize request
    ibv_mr* msg_mr = ibv_reg_mr(
      conn_id_->pd,
      msg_buf,
      alloc_len,
      IBV_ACCESS_LOCAL_WRITE
    );
    CHECK(msg_mr != nullptr);

    // Get the memory region info of server. 
    while(*ptr_tag != '1') {}
    RemoteInfo remote_info;
    memcpy(&remote_info, reinterpret_cast<char*>(
      block_addr), sizeof(remote_info));
    shared_res_->ReturnOneBlock(block_addr);

    // Write the large message to server.
    uint32_t call_id = 0;
    if (msg_len >= max_threshold) call_id = corr_id;
    WriteLargeMessage(
      conn_id_->qp, msg_mr, call_id, remote_info);
  }
  // For synchronous rpc, we need to wait for the response.
  if (done == nullptr) CHECK(corr_future.get());
}

LightChannel::~LightChannel() {
  CHECK(rdma_disconnect(conn_id_) == 0);
  rdma_cm_event *cm_ent = nullptr;
  CHECK(rdma_get_cm_event(ent_chan_, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_DISCONNECTED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  WriteLock locker(shared_res_->shared_mtx_);
  shared_res_->client_map_.erase(conn_id_->qp->qp_num);
  locker.unlock();

  delete reinterpret_cast<QueuePairContext*>(
    conn_id_->qp->qp_context);
  rdma_destroy_qp(conn_id_);
  CHECK(rdma_destroy_id(conn_id_) == 0);
  rdma_destroy_event_channel(ent_chan_);
}

} // namespace lightrpc