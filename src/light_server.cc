#include "src/light_server.h"
#include "src/light_api.h"
#include "src/light_controller.h"
#include "proto/light_impl.pb.h"

#include <mimalloc/mimalloc.h>

namespace lightrpc {

LightServer::LightServer(ResourceConfig config, int num_client)
: SharedResource(config), num_clients_(num_client) {
  // Post some recv requests in advance.
  int post_num = srq_max_wr;
  CHECK(post_num <= num_blocks_ / 2);
  for (int i = 0; i < post_num; i++) {
    uint64_t recv_addr = 0;
    ObtainOneBlock(recv_addr);
    PostOneRecvRequest(recv_addr);
  }

  CHECK(rdma_listen(cm_id_, listen_backlog) == 0);
  LOG_INFO("Start listening on port %d", config_.local_port);
}

void LightServer::AddService(ServiceOwnership ownership, 
                             google::protobuf::Service* service) {
  std::string name = service->GetDescriptor()->name();
  if (service_map_.find(name) != service_map_.end()) {
    LOG_ERR("Add service repeatedly.");
    exit(EXIT_FAILURE);
  }
  ServiceInfo service_info = {
    .ownership = ownership, 
    .service = service 
  };
  service_map_.insert({name, service_info});
}

void LightServer::BuildAndStart() {
  int client_count = 0;
  rdma_cm_event_type ent_type;
  rdma_cm_id* conn_id = nullptr;
  rdma_cm_event* cm_ent = nullptr;
  while (true) {
    CHECK(rdma_get_cm_event(ent_chan_, &cm_ent) == 0);
    ent_type = cm_ent->event; conn_id = cm_ent->id;
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
      if (++client_count == num_clients_) break;
    } else LOG_ERR("Other event value: %d.", ent_type);
  }
}

LightServer::~LightServer() {
  for(auto &kv : service_map_) {
    if (kv.second.ownership == SERVER_OWNS_SERVICE)
      delete kv.second.service;
  }
}

void LightServer::ProcessConnectRequest(rdma_cm_id* conn_id) {
  CreateQueuePair(conn_id);
  // Record the mapping of qp number to qp.
  WriteLock locker(shared_mtx_);
  qp_num_map_.insert({conn_id->qp->qp_num, conn_id->qp});
  locker.unlock();
  // Accept the connection request.
  rdma_conn_param cm_params;
  memset(&cm_params, 0, sizeof(cm_params));
  // cm_params.rnr_retry_count = 7;  // infinite retry
  CHECK(rdma_accept(conn_id, &cm_params) == 0);
}

void LightServer::ProcessConnectEstablish(rdma_cm_id* conn_id) {
  auto addr = reinterpret_cast<sockaddr_in*>(
    rdma_get_peer_addr(conn_id));
  CHECK(addr != nullptr);
  LOG_INFO("Connection from <IP: %s, Port: %hu> established.", 
    inet_ntoa(addr->sin_addr), ntohs(addr->sin_port));
}

void LightServer::ProcessDisconnect(rdma_cm_id* conn_id) {
  auto addr = reinterpret_cast<sockaddr_in*>(
    rdma_get_peer_addr(conn_id));
  CHECK(addr != nullptr);

  WriteLock locker(shared_mtx_);
  qp_num_map_.erase(conn_id->qp->qp_num);
  locker.unlock();

  delete reinterpret_cast<QueuePairContext*>(
    conn_id->qp->qp_context);
  rdma_destroy_qp(conn_id);
  CHECK(rdma_destroy_id(conn_id) == 0);
  
  LOG_INFO("Connection from <IP: %s, Port: %hu> disconnected.", 
    inet_ntoa(addr->sin_addr), ntohs(addr->sin_port));
}

void LightServer::ProcessRecvWorkCompletion(ibv_wc& wc) {
  uint32_t qp_num = wc.qp_num;
  uint64_t recv_addr = wc.wr_id;
  uint32_t imm_data = ntohl(wc.imm_data);
  ibv_wc_opcode opcode = wc.opcode;
  service_pool_[round_robin_idx_]->post(
    [this, qp_num, recv_addr, imm_data, opcode] {
    ReadLock locker(shared_mtx_);
    auto it = qp_num_map_.find(qp_num);
    CHECK(it != qp_num_map_.end());
    ibv_qp* conn_qp = it->second;
    locker.unlock();

    // The imm_data is rpc_id.
    if (opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
      ReturnOneBlock(recv_addr);
      ibv_mr* msg_mr = GetMemoryRegion(imm_data, conn_qp);
      ParseAndRunFunc(conn_qp, msg_mr, 
        static_cast<uint32_t>(msg_mr->length));
      return;
    }

    // The imm_data is message length.
    CHECK(opcode == IBV_WC_RECV);
    if (imm_data <= msg_threshold) {
      uint64_t addr = 0;
      ObtainOneBlock(addr);
      PostOneRecvRequest(addr);
      char* msg_addr = reinterpret_cast<char*>(recv_addr);
      ParseAndRunFunc(conn_qp, msg_addr, imm_data);
    } else {
      ibv_mr* msg_mr = AllocLargeMsgMR(imm_data, conn_qp);
      ProcessLargeMessage(
        conn_qp, msg_mr, recv_addr, imm_data, SERVER_FLAG);
      if (imm_data >= max_threshold) return;
      else ParseAndRunFunc(conn_qp, msg_mr, imm_data);
    }
  });
  round_robin_idx_ = (round_robin_idx_ + 1) % config_.num_threads;
}

void LightServer::ParseAndRunFunc(ibv_qp* conn_qp,
                                  void* addr,
                                  uint32_t msg_len) {
  char* msg_addr = nullptr;
  if (msg_len <= msg_threshold) {
    msg_addr = reinterpret_cast<char*>(addr);
  } else {
    msg_addr = reinterpret_cast<char*>(
      reinterpret_cast<ibv_mr*>(addr)->addr);
  }

  uint16_t metadata_size = 0;
  memcpy(&metadata_size, msg_addr, req_fixed_len);

  // Parse and get metadata.
  RequestMetadata metadata;
  CHECK(metadata.ParseFromArray(
    msg_addr + req_fixed_len, metadata_size));
  uint32_t rpc_id = metadata.rpc_id();
  std::string service_name = metadata.service_name();
  std::string method_name = metadata.method_name();

  auto iter = service_map_.find(service_name);
  auto service = iter->second.service;
  auto method = 
    service->GetDescriptor()->FindMethodByName(method_name);
  
  // Create request and response instance.
  auto request = service->GetRequestPrototype(method).New();
  auto response = service->GetResponsePrototype(method).New();

  uint32_t request_size = 
    msg_len - req_fixed_len - metadata_size;
  CHECK(request->ParseFromArray(
    msg_addr + req_fixed_len + metadata_size, request_size));
  
  // Release the occupied resources.
  if (msg_len <= msg_threshold) 
    ReturnOneBlock(reinterpret_cast<uint64_t>(msg_addr));
  if (msg_len >= max_threshold) {
    CHECK(ibv_dereg_mr(
      reinterpret_cast<ibv_mr*>(addr)) == 0);
    free(msg_addr);
  }

  CallBackArgs args = {
    .rpc_id = rpc_id,
    .conn_qp = conn_qp, 
    .request = request, 
    .response = response 
  };

  auto done = google::protobuf::NewCallback(
    this, &LightServer::ReturnRpcResult, args);
  
  LightController controller;
  service->CallMethod(
    method, &controller, request, response, done);
}

void LightServer::ReturnRpcResult(CallBackArgs args) {
  CHECK(args.response != nullptr);
  // Use RAII mechanism to release resources.
  std::unique_ptr<google::protobuf::Message> 
    request_guard(args.request);
  std::unique_ptr<google::protobuf::Message> 
    response_guard(args.response);
  
  uint32_t response_size = args.response->ByteSizeLong();
  uint32_t msg_len = res_head_len + response_size;

  char* msg_buf = nullptr;
  auto SerializeFunc = [&]() {
    memcpy(msg_buf, &args.rpc_id, res_head_len);
    CHECK(args.response->SerializeToArray(
      msg_buf + res_head_len, response_size));
    if (msg_len > msg_threshold && msg_len < max_threshold)
      memset(msg_buf + msg_len, '1', 1);
  };

  // For small msg, server will copy it to the block.
  // For large msg, client will write mr info to the block.
  uint64_t block_addr = 0;
  ObtainOneBlock(block_addr);

  if (msg_len <= msg_threshold) {
    msg_buf = reinterpret_cast<char*>(
      mi_malloc(msg_len));
    CHECK(msg_buf != nullptr);
    SerializeFunc(); // serialize response
    memcpy(reinterpret_cast<char*>(
      block_addr), msg_buf, msg_len);
    mi_free(msg_buf);
    SendSmallMessage(args.conn_qp, block_addr, 
      msg_len, shared_mr_->lkey);
  } else {
    // Send the msg length and remote info.
    char* ptr_tag = reinterpret_cast<char*>(
      block_addr) + sizeof(RemoteInfo);
    memset(ptr_tag, '0', 1);

    if (msg_len >= max_threshold) {
      RemoteInfoWithId info = {
        .rpc_id = args.rpc_id,
        .remote_key = shared_mr_->rkey,
        .remote_addr = block_addr
      };
      SendNotifyMessage(args.conn_qp, msg_len, info);
    } else {
      RemoteInfo info = {
        .remote_key = shared_mr_->rkey,
        .remote_addr = block_addr
      };
      SendNotifyMessage(args.conn_qp, msg_len, info);
    }

    // Allocate memory, serialize and register.
    uint32_t alloc_len = 0;
    if (msg_len >= max_threshold) alloc_len = msg_len;
    else alloc_len = msg_len + 1;
    msg_buf = reinterpret_cast<char*>(
      mi_malloc(alloc_len));
    CHECK(msg_buf != nullptr);
    SerializeFunc(); // serialize response
    ibv_mr* msg_mr = ibv_reg_mr(
      args.conn_qp->pd,
      msg_buf,
      alloc_len,
      IBV_ACCESS_LOCAL_WRITE
    );
    CHECK(msg_mr != nullptr);

    // Get the memory region info of client. 
    while(*ptr_tag != '1') {}
    RemoteInfo remote_info;
    memcpy(&remote_info, reinterpret_cast<char*>(
      block_addr), sizeof(remote_info));
    ReturnOneBlock(block_addr);

    // Write the large message to client.
    uint32_t call_id = 0;
    if (msg_len >= max_threshold) call_id = args.rpc_id;
    WriteLargeMessage(
      args.conn_qp, msg_mr, call_id, remote_info);
  }
}

} // namespace lightrpc