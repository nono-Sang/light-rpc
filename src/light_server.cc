#include <mimalloc/mimalloc.h>

#include "light_control.h"
#include "light_def.h"
#include "light_impl.pb.h"
#include "light_log.h"
#include "light_server.h"

namespace lightrpc {

LightServer::LightServer(ResourceConfig config) : GlobalResource(config) {
  /// NOTE: Post some recv requests in advance.
  int post_num = srq_max_wr;
  CHECK(post_num < num_blocks_ / 2);
  for (int i = 0; i < post_num; i++) {
    uint64_t recv_addr = 0;
    ObtainOneBlock(recv_addr);
    PostOneRecvRequest(recv_addr);
  }

  CHECK(rdma_listen(cm_id_, listen_backlog) == 0);
  LOG_INFO("Start listening on port %d", config_.local_port);
}

LightServer::~LightServer() {
  for (auto &kv : service_map_) {
    if (kv.second.ownership == SERVER_OWNS_SERVICE) delete kv.second.service;
  }
}

void LightServer::AddService(ServiceOwnership ownership, google::protobuf::Service *service) {
  std::string name = service->GetDescriptor()->name();
  if (service_map_.find(name) != service_map_.end()) {
    LOG_ERR("Add service repeatedly.");
    exit(EXIT_FAILURE);
  }
  ServiceInfo service_info = {.ownership = ownership, .service = service};
  service_map_.insert({name, service_info});
}

void LightServer::BuildAndStart() {
  int client_count = 0;
  rdma_cm_event_type ent_type;
  rdma_cm_id *conn_id = nullptr;
  rdma_cm_event *cm_ent = nullptr;
  while (true) {
    CHECK(rdma_get_cm_event(cm_id_->channel, &cm_ent) == 0);
    ent_type = cm_ent->event;
    conn_id = cm_ent->id;
    if (ent_type == RDMA_CM_EVENT_CONNECT_REQUEST) {
      CHECK(rdma_ack_cm_event(cm_ent) == 0);
      CHECK(conn_id->verbs == cm_id_->verbs);
      CHECK(conn_id->pd == cm_id_->pd);
      ProcessConnectRequest(conn_id);
    } else if (ent_type == RDMA_CM_EVENT_ESTABLISHED) {
      CHECK(rdma_ack_cm_event(cm_ent) == 0);
      ProcessConnectEstablish(conn_id);
    } else if (ent_type == RDMA_CM_EVENT_DISCONNECTED) {
      CHECK(rdma_ack_cm_event(cm_ent) == 0);
      ProcessDisconnect(conn_id);
    } else {
      LOG_ERR("Other event value: %d.", ent_type);
    }
  }
}

void LightServer::ProcessConnectRequest(rdma_cm_id *conn_id) {
  // Create queue pair and qp context.
  CreateQueuePair(conn_id);
  // Record the mapping of qp number to qp.
  {
    std::lock_guard<std::mutex> locker(qp_num_mtx_);
    qp_num_map_.insert({conn_id->qp->qp_num, conn_id->qp});
  }
  // Accept the connection request.
  rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.rnr_retry_count = 7;  // infinite retry
  CHECK(rdma_accept(conn_id, &cm_params) == 0);
}

void LightServer::ProcessConnectEstablish(rdma_cm_id *conn_id) {
  auto addr = reinterpret_cast<sockaddr_in *>(rdma_get_peer_addr(conn_id));
  LOG_INFO("Connection from <IP: %s, Port: %hu> established.",
           inet_ntoa(addr->sin_addr),
           ntohs(addr->sin_port));
}

void LightServer::ProcessDisconnect(rdma_cm_id *conn_id) {
  auto addr = reinterpret_cast<sockaddr_in *>(rdma_get_peer_addr(conn_id));
  {
    std::lock_guard<std::mutex> locker(qp_num_mtx_);
    qp_num_map_.erase(conn_id->qp->qp_num);
  }
  delete reinterpret_cast<QueuePairContext *>(conn_id->qp->qp_context);
  rdma_destroy_qp(conn_id);
  CHECK(rdma_destroy_id(conn_id) == 0);
  LOG_INFO("Connection from <IP: %s, Port: %hu> disconnected.",
           inet_ntoa(addr->sin_addr),
           ntohs(addr->sin_port));
}

void LightServer::ProcessRecvWorkCompletion(ibv_wc &wc) {
  ibv_qp *conn_qp = nullptr;
  {
    std::lock_guard<std::mutex> locker(qp_num_mtx_);
    auto it = qp_num_map_.find(wc.qp_num);
    CHECK(it != qp_num_map_.end());
    conn_qp = it->second;
  }

  int goal_thread_id = SelectTargetThread();
  auto thread_id = thread_pool_[goal_thread_id].get_id();
  thread_info_map_[thread_id]->IncTotalCount();

  uint32_t imm_data = ntohl(wc.imm_data);
  uint64_t recv_addr = wc.wr_id;
  ibv_wc_opcode opcode = wc.opcode;

  service_pool_[goal_thread_id]->post([this, conn_qp, imm_data, recv_addr, opcode] {
    auto thread_id = std::this_thread::get_id();
    if (opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
      /// Received large message (imm_data is rpc_id).
      /// NOTE: For next rpc request (reuse block).
      memset(reinterpret_cast<void *>(recv_addr), 0, msg_threshold);
      PostOneRecvRequest(recv_addr);
      ibv_mr *msg_mr = GetAndEraseMemoryRegion(imm_data, conn_qp);
      uint32_t msg_len = static_cast<uint32_t>(msg_mr->length);
      ParseRequest(conn_qp, msg_mr, msg_len);
    } else if (imm_data == 0) {
      /// Received authority message (imm_data is 0).
      ProcessAuthorityMessage(recv_addr, conn_qp);
    } else if (imm_data <= msg_threshold) {
      /// Received small message (imm_data is msg_len).
      /// NOTE: For next rpc request.
      GetAndPostOneBlock();
      char *msg_addr = reinterpret_cast<char *>(recv_addr);
      ParseRequest(conn_qp, msg_addr, imm_data);
    } else {
      /// Received notify message (imm_data is msg_len).
      ProcessNotifyMessage(imm_data, recv_addr, conn_qp);
    }
    thread_info_map_[thread_id]->IncCompleteCount();
  });
}

void LightServer::ParseRequest(ibv_qp *conn_qp, void *addr, uint32_t msg_len) {
  char *msg_addr = nullptr;
  if (msg_len <= msg_threshold) {
    msg_addr = reinterpret_cast<char *>(addr);
  } else {
    auto mr = reinterpret_cast<ibv_mr *>(addr);
    msg_addr = reinterpret_cast<char *>(mr->addr);
  }

  RequestHead head;
  head.ParseFromArray(msg_addr, max_uint32_field);  // no check
  uint32_t descrip_len = head.descrip_size();

  RequestDescrip descrip;
  CHECK(descrip.ParseFromArray(msg_addr + max_uint32_field, descrip_len));
  uint32_t rpc_id = descrip.rpc_id();
  std::string service_name = descrip.service_name();
  std::string method_name = descrip.method_name();

  auto iter = service_map_.find(service_name);
  auto service = iter->second.service;
  auto method = service->GetDescriptor()->FindMethodByName(method_name);

  // Create request and response instance.
  auto request = service->GetRequestPrototype(method).New();
  auto response = service->GetResponsePrototype(method).New();

  uint32_t req_len = msg_len - max_uint32_field - descrip_len;
  CHECK(request->ParseFromArray(msg_addr + max_uint32_field + descrip_len, req_len));

  // Release the occupied resources.
  if (msg_len <= msg_threshold) {
    ReturnOneBlock(reinterpret_cast<uint64_t>(msg_addr));
  } else {
    CHECK(ibv_dereg_mr(reinterpret_cast<ibv_mr *>(addr)) == 0);
    mi_free(msg_addr);
  }

  CallBackArgs args = {
      .rpc_id = rpc_id, .conn_qp = conn_qp, .request = request, .response = response};

  auto done = google::protobuf::NewCallback(this, &LightServer::ReturnResponse, args);

  LightController controller;
  service->CallMethod(method, &controller, request, response, done);
}

void LightServer::ReturnResponse(CallBackArgs args) {
  // Use RAII mechanism to release resources.
  std::unique_ptr<google::protobuf::Message> request_guard(args.request);
  std::unique_ptr<google::protobuf::Message> response_guard(args.response);

  // Message Layout: ResponseHead | ResponseBody
  uint32_t res_len = args.response->ByteSizeLong();
  uint32_t head_len = max_uint32_field;

  // The total length of the message to be sent.
  uint32_t msg_len = head_len + res_len;

  ResponseHead head;
  head.set_rpc_id(args.rpc_id);

  // Define the serialization lambda function.
  char *msg_buf = nullptr;
  auto serialize_func = [&] {
    msg_buf = reinterpret_cast<char *>(mi_malloc(msg_len));
    CHECK(msg_buf != nullptr);
    CHECK(head.SerializeToArray(msg_buf, head_len));
    CHECK(args.response->SerializeToArray(msg_buf + head_len, res_len));
  };

  GenericSendFunc(msg_len, msg_buf, serialize_func, args.rpc_id, args.conn_qp);
}

}  // namespace lightrpc