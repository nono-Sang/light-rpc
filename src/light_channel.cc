#include <mimalloc/mimalloc.h>

#include "light_channel.h"
#include "light_def.h"
#include "light_impl.pb.h"
#include "light_log.h"

namespace lightrpc {

LightChannel::LightChannel(std::string dest_ip,
                           uint16_t dest_port,
                           ClientGlobalResource *global_res)
    : global_res_(global_res), rpc_id_(0) {
  auto temp_channel = rdma_create_event_channel();
  CHECK(temp_channel != nullptr);
  CHECK(rdma_create_id(temp_channel, &conn_id_, nullptr, RDMA_PS_TCP) == 0);

  // Resolve remote address and bind local device.
  sockaddr_in dest_addr;
  memset(&dest_addr, 0, sizeof(dest_addr));
  dest_addr.sin_family = AF_INET;
  dest_addr.sin_addr.s_addr = inet_addr(dest_ip.c_str());
  dest_addr.sin_port = htons(dest_port);

  sockaddr_in local_addr;
  memset(&local_addr, 0, sizeof(local_addr));
  local_addr.sin_family = AF_INET;
  local_addr.sin_addr.s_addr = inet_addr(global_res_->config_.local_ip.c_str());

  CHECK(rdma_resolve_addr(conn_id_,
                          reinterpret_cast<sockaddr *>(&local_addr),
                          reinterpret_cast<sockaddr *>(&dest_addr),
                          timeout_in_ms) == 0);

  rdma_cm_event *cm_ent = nullptr;
  CHECK(rdma_get_cm_event(conn_id_->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ADDR_RESOLVED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  CHECK(conn_id_->verbs == global_res_->cm_id_->verbs);
  CHECK(conn_id_->pd == global_res_->cm_id_->pd);

  // Create queue pair and qp context.
  global_res_->CreateQueuePair(conn_id_);
  {
    std::lock_guard<std::mutex> locker(global_res_->client_mtx_);
    global_res_->client_map_.insert({conn_id_->qp->qp_num, this});
  }

  // Resolve an RDMA route to the destination address.
  CHECK(rdma_resolve_route(conn_id_, timeout_in_ms) == 0);
  CHECK(rdma_get_cm_event(conn_id_->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  // Connect to remote address.
  rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.rnr_retry_count = 7;  // infinite retry
  CHECK(rdma_connect(conn_id_, &cm_params) == 0);
  CHECK(rdma_get_cm_event(conn_id_->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_ESTABLISHED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);

  LOG_INFO("[Client] Connection to <IP: %s, Port: %hu> established.", dest_ip.c_str(), dest_port);
}

LightChannel::~LightChannel() {
  CHECK(rdma_disconnect(conn_id_) == 0);
  rdma_cm_event *cm_ent = nullptr;
  CHECK(rdma_get_cm_event(conn_id_->channel, &cm_ent) == 0);
  CHECK(cm_ent->event == RDMA_CM_EVENT_DISCONNECTED);
  CHECK(rdma_ack_cm_event(cm_ent) == 0);
  {
    std::lock_guard<std::mutex> locker(global_res_->client_mtx_);
    global_res_->client_map_.erase(conn_id_->qp->qp_num);
  }
  delete reinterpret_cast<QueuePairContext *>(conn_id_->qp->qp_context);
  rdma_destroy_qp(conn_id_);
  auto temp_chan = conn_id_->channel;
  CHECK(rdma_destroy_id(conn_id_) == 0);
  rdma_destroy_event_channel(temp_chan);
}

void LightChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                              google::protobuf::RpcController *controller,
                              const google::protobuf::Message *request,
                              google::protobuf::Message *response,
                              google::protobuf::Closure *done) {
  // Set rpc call id.
  uint32_t curr_id = 0;
  {
    std::lock_guard<std::mutex> locker(id_mtx_);
    curr_id = ++rpc_id_;
  }

  // Record the call info in the hashmap.
  std::promise<bool> curr_prom;
  std::future<bool> curr_future = curr_prom.get_future();
  RpcCallInfo info = {curr_prom, response, done};
  {
    std::lock_guard<std::mutex> locker(cinfo_mtx_);
    cinfo_map_.insert({curr_id, info});
  }

  // Message Layout: RequestHead | RequestDescrip | RequestBody
  uint32_t req_len = request->ByteSizeLong();
  RequestDescrip descrip;
  descrip.set_rpc_id(curr_id);
  descrip.set_service_name(method->service()->name());
  descrip.set_method_name(method->name());
  uint32_t descrip_len = descrip.ByteSizeLong();
  RequestHead head;
  head.set_descrip_size(descrip_len);
  uint32_t head_len = max_uint32_field;

  // The total length of the message to be sent.
  uint32_t msg_len = head_len + descrip_len + req_len;

  // Define the serialization lambda function.
  char *msg_buf = nullptr;
  auto serialize_func = [&] {
    msg_buf = reinterpret_cast<char *>(mi_malloc(msg_len));
    CHECK(msg_buf != nullptr);
    char *descrip_offset = msg_buf + head_len;
    char *req_offset = descrip_offset + descrip_len;
    CHECK(head.SerializeToArray(msg_buf, head_len));
    CHECK(descrip.SerializeToArray(descrip_offset, descrip_len));
    CHECK(request->SerializeToArray(req_offset, req_len));
  };

  /// NOTE: For receiving response.
  global_res_->GetAndPostOneBlock();
  global_res_->GenericSendFunc(msg_len, msg_buf, serialize_func, curr_id, conn_id_->qp);

  // For synchronous rpc, we need to wait for the response.
  if (done == nullptr) CHECK(curr_future.get());
}

void ClientGlobalResource::ProcessRecvWorkCompletion(ibv_wc &wc) {
  LightChannel *client = nullptr;
  {
    std::lock_guard<std::mutex> locker(client_mtx_);
    auto it = client_map_.find(wc.qp_num);
    CHECK(it != client_map_.end());
    client = it->second;
  }

  int goal_thread_id = SelectTargetThread();
  auto thread_id = thread_pool_[goal_thread_id].get_id();
  thread_info_map_[thread_id]->IncTotalCount();

  uint32_t imm_data = ntohl(wc.imm_data);
  uint64_t recv_addr = wc.wr_id;
  ibv_wc_opcode opcode = wc.opcode;

  service_pool_[goal_thread_id]->post([this, client, imm_data, recv_addr, opcode] {
    auto thread_id = std::this_thread::get_id();
    auto conn_qp = client->conn_id_->qp;
    if (opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
      /// Received large message (imm_data is rpc_id).
      /// NOTE: Return the block.
      ReturnOneBlock(recv_addr);
      ibv_mr *msg_mr = GetAndEraseMemoryRegion(imm_data, conn_qp);
      uint32_t msg_len = static_cast<uint32_t>(msg_mr->length);
      ParseResponse(client, msg_mr, msg_len);
    } else if (imm_data == 0) {
      /// Received authority message (imm_data is 0).
      ProcessAuthorityMessage(recv_addr, conn_qp);
    } else if (imm_data <= msg_threshold) {
      /// Received small message (imm_data is msg_len).
      char *msg_addr = reinterpret_cast<char *>(recv_addr);
      ParseResponse(client, msg_addr, imm_data);
    } else {
      /// Received notify message (imm_data is msg_len).
      ProcessNotifyMessage(imm_data, recv_addr, conn_qp);
    }
    thread_info_map_[thread_id]->IncCompleteCount();
  });
}

void ClientGlobalResource::ParseResponse(LightChannel *client, void *addr, uint32_t msg_len) {
  char *msg_addr = nullptr;
  if (msg_len <= msg_threshold) {
    msg_addr = reinterpret_cast<char *>(addr);
  } else {
    auto mr = reinterpret_cast<ibv_mr *>(addr);
    msg_addr = reinterpret_cast<char *>(mr->addr);
  }

  ResponseHead head;
  head.ParseFromArray(msg_addr, max_uint32_field);  // no check

  uint32_t rpc_id = head.rpc_id();
  std::unique_lock<std::mutex> locker(client->cinfo_mtx_);
  auto it = client->cinfo_map_.find(rpc_id);
  CHECK(it != client->cinfo_map_.end());
  RpcCallInfo &callinfo = it->second;
  locker.unlock();

  uint32_t res_len = msg_len - max_uint32_field;
  CHECK(callinfo.response->ParseFromArray(msg_addr + max_uint32_field, res_len));

  if (msg_len <= msg_threshold) {
    ReturnOneBlock(reinterpret_cast<uint64_t>(msg_addr));
  } else {
    CHECK(ibv_dereg_mr(reinterpret_cast<ibv_mr *>(addr)) == 0);
    mi_free(msg_addr);
  }

  if (callinfo.callback) {
    // For asynchronous rpc, run the callback.
    callinfo.callback->Run();
  } else {
    // For synchronous rpc, notify the user thread.
    callinfo.resback.set_value(true);
  }

  std::lock_guard<std::mutex> lk(client->cinfo_mtx_);
  client->cinfo_map_.erase(rpc_id);
}

}  // namespace lightrpc