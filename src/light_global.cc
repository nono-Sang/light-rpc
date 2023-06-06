#include <mimalloc/mimalloc.h>

#include "light_def.h"
#include "light_global.h"
#include "light_impl.pb.h"
#include "light_log.h"

namespace lightrpc {

void QueryDeviceAttribute(ibv_context *ctx) {
  ibv_device_attr dev_attr;
  CHECK(ibv_query_device(ctx, &dev_attr) == 0);
  LOG_INFO("######### Device Info #########");
  LOG_INFO("max_pd: %d", dev_attr.max_pd);
  LOG_INFO("max_mr: %d", dev_attr.max_mr);
  LOG_INFO("max_mr_size: %lu", dev_attr.max_mr_size);
  LOG_INFO("max_qp: %d", dev_attr.max_qp);
  LOG_INFO("max_qp_wr: %d", dev_attr.max_qp_wr);
  LOG_INFO("max_srq: %d", dev_attr.max_srq);
  LOG_INFO("max_srq_wr: %d", dev_attr.max_srq_wr);
  LOG_INFO("max_cq: %d", dev_attr.max_cq);
  LOG_INFO("max_cqe: %d", dev_attr.max_cqe);
  LOG_INFO("###############################");
}

ibv_mr *AllocMemoryRegion(uint32_t msg_len, ibv_pd *pd) {
  ibv_mr *msg_mr = nullptr;
  void *msg_addr = mi_malloc(msg_len);
  CHECK(msg_addr != nullptr);
  msg_mr = ibv_reg_mr(pd, msg_addr, msg_len, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  CHECK(msg_mr != nullptr);
  return msg_mr;
}

void SendSmallMessage(ibv_qp *qp, uint64_t block_addr, uint32_t msg_len, uint32_t lkey) {
  ibv_sge send_sg = {.addr = block_addr, .length = msg_len, .lkey = lkey};

  ibv_send_wr send_wr;
  ibv_send_wr *send_bad_wr = nullptr;
  memset(&send_wr, 0, sizeof(send_wr));
  // Set wr_id to block address.
  send_wr.wr_id = block_addr;
  send_wr.num_sge = 1;
  send_wr.sg_list = &send_sg;
  send_wr.imm_data = htonl(msg_len);
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = IBV_SEND_SIGNALED;  // wc
  CHECK(ibv_post_send(qp, &send_wr, &send_bad_wr) == 0);
}

void SendControlMessage(ibv_qp *qp, void *send_addr, uint32_t send_len, uint32_t imm_data) {
  ibv_sge send_sg = {.addr = reinterpret_cast<uint64_t>(send_addr), .length = send_len};

  ibv_send_wr send_wr;
  ibv_send_wr *send_bad_wr = nullptr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.num_sge = 1;
  send_wr.sg_list = &send_sg;
  send_wr.imm_data = htonl(imm_data);
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = IBV_SEND_INLINE;  // no wc
  CHECK(ibv_post_send(qp, &send_wr, &send_bad_wr) == 0);
}

void WriteLargeMessage(ibv_qp *qp, ibv_mr *msg_mr, uint32_t rpc_id, RemoteInfo &target) {
  ibv_sge write_sg = {.addr = reinterpret_cast<uint64_t>(msg_mr->addr),
                      .length = static_cast<uint32_t>(msg_mr->length),
                      .lkey = msg_mr->lkey};

  ibv_send_wr write_wr;
  ibv_send_wr *write_bad_wr = nullptr;
  memset(&write_wr, 0, sizeof(write_wr));
  // Set wr_id to mr address.
  write_wr.wr_id = reinterpret_cast<uint64_t>(msg_mr);
  write_wr.num_sge = 1;
  write_wr.sg_list = &write_sg;
  write_wr.imm_data = htonl(rpc_id);
  write_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  write_wr.send_flags = IBV_SEND_SIGNALED;  // wc
  write_wr.wr.rdma.rkey = target.remote_key;
  write_wr.wr.rdma.remote_addr = target.remote_addr;
  CHECK(ibv_post_send(qp, &write_wr, &write_bad_wr) == 0);
}

GlobalResource::GlobalResource(const ResourceConfig &config)
    : config_(config),
      num_blocks_(config_.block_pool_size / msg_threshold),
      addr_queue_(num_blocks_),
      handler_work_(handler_service_) {
  CreateControlID();
  BindLocalDevice();
  CreateBlockPool();
  CreateSharedQueue();
  StartWorkerThread();
  StartPollerThread();
}

GlobalResource::~GlobalResource() {
  for (int i = 0; i < config_.num_threads; i++) {
    auto thread_id = thread_pool_[i].get_id();
    service_pool_[i]->stop();
    thread_pool_[i].join();
    delete thread_info_map_[thread_id];
    delete work_pool_[i];
    delete service_pool_[i];
  }

  poller_stop_ = true;
  wc_poller_.join();
  handler_service_.stop();
  wc_handler_.join();

  auto addr_ptr = shared_mr_->addr;
  CHECK(ibv_dereg_mr(shared_mr_) == 0);
  mi_free(addr_ptr);

  CHECK(ibv_destroy_srq(shared_rq_) == 0);
  CHECK(ibv_destroy_cq(shared_send_cq_) == 0);
  CHECK(ibv_destroy_cq(shared_recv_cq_) == 0);
  auto temp_chan = cm_id_->channel;
  CHECK(rdma_destroy_id(cm_id_) == 0);
  rdma_destroy_event_channel(temp_chan);
}

void GlobalResource::ObtainOneBlock(uint64_t &addr) {
  // Set non-blocking to blocking.
  while (addr_queue_.pop(addr) == false) {
    LOG_INFO("wait...");
  }
}

void GlobalResource::ReturnOneBlock(uint64_t addr) {
  memset(reinterpret_cast<void *>(addr), 0, msg_threshold);
  addr_queue_.push(addr);
}

void GlobalResource::CreateControlID() {
  auto temp_channel = rdma_create_event_channel();
  CHECK(temp_channel != nullptr);
  CHECK(rdma_create_id(temp_channel, &cm_id_, nullptr, RDMA_PS_TCP) == 0);
}

void GlobalResource::BindLocalDevice() {
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(config_.local_ip.c_str());
  if (config_.local_port >= 0) {
    addr.sin_port = htons(config_.local_port);
  }
  CHECK(rdma_bind_addr(cm_id_, reinterpret_cast<sockaddr *>(&addr)) == 0);
  // Check whether the verbs and pd have been obtained.
  CHECK(cm_id_->verbs != nullptr);
  CHECK(cm_id_->pd != nullptr);
  // QueryDeviceAttribute(cm_id_->verbs);
}

void GlobalResource::CreateBlockPool() {
  shared_mr_ = AllocMemoryRegion(config_.block_pool_size, cm_id_->pd);
  auto addr_ptr = shared_mr_->addr;
  memset(addr_ptr, 0, config_.block_pool_size);

  uint64_t mr_addr = reinterpret_cast<uint64_t>(addr_ptr);
  for (int i = 0; i < num_blocks_; i++) {
    addr_queue_.unsynchronized_push(mr_addr + i * msg_threshold);
  }
}

void GlobalResource::CreateSharedQueue() {
  ibv_srq_init_attr srq_init_attr;
  memset(&srq_init_attr, 0, sizeof(srq_init_attr));
  // NOTE: srq_limit is ignored in Infiniband.
  srq_init_attr.attr.max_sge = srq_max_sge;
  srq_init_attr.attr.max_wr = srq_max_wr;
  shared_rq_ = ibv_create_srq(cm_id_->pd, &srq_init_attr);
  CHECK(shared_rq_ != nullptr);

  shared_send_cq_ = ibv_create_cq(cm_id_->verbs, min_cqe_num, nullptr, nullptr, 0);
  CHECK(shared_send_cq_ != nullptr);
  shared_recv_cq_ = ibv_create_cq(cm_id_->verbs, min_cqe_num, nullptr, nullptr, 0);
  CHECK(shared_recv_cq_ != nullptr);
}

void GlobalResource::StartWorkerThread() {
  thread_pool_.resize(config_.num_threads);
  service_pool_.resize(config_.num_threads);
  work_pool_.resize(config_.num_threads);

  for (int i = 0; i < config_.num_threads; i++) {
    service_pool_[i] = new boost::asio::io_context;
    work_pool_[i] = new boost::asio::io_context::work(*service_pool_[i]);
    thread_pool_[i] = std::thread([this, i] { this->service_pool_[i]->run(); });

    auto thread_id = thread_pool_[i].get_id();
    auto thread_info = new ThreadInfo(i);
    thread_info_map_.insert({thread_id, thread_info});
  }

  // Start the send wc handler thread.
  wc_handler_ = std::thread([this] { this->handler_service_.run(); });
}

void GlobalResource::StartPollerThread() {
  poller_stop_ = false;
  wc_poller_ = std::thread([this] { this->PollWorkCompletion(); });
}

void GlobalResource::CreateQueuePair(rdma_cm_id *conn_id) {
  ibv_qp_init_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.sq_sig_all = 0;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.srq = shared_rq_;
  qp_attr.send_cq = shared_send_cq_;
  qp_attr.recv_cq = shared_recv_cq_;
  qp_attr.cap.max_send_wr = max_send_wr;
  qp_attr.cap.max_send_sge = max_send_sge;
  qp_attr.cap.max_inline_data = max_inline_data;
  CHECK(rdma_create_qp(conn_id, conn_id->pd, &qp_attr) == 0);

  conn_id->qp->qp_context = reinterpret_cast<void *>(new QueuePairContext);
}

void GlobalResource::PostOneRecvRequest(uint64_t recv_addr) {
  ibv_sge recv_sg = {.addr = recv_addr, .length = msg_threshold, .lkey = shared_mr_->lkey};
  ibv_recv_wr recv_wr;
  ibv_recv_wr *recv_bad_wr = nullptr;
  memset(&recv_wr, 0, sizeof(recv_wr));
  // Store recv address in wr_id field.
  recv_wr.wr_id = recv_addr;
  recv_wr.num_sge = 1;
  recv_wr.sg_list = &recv_sg;
  CHECK(ibv_post_srq_recv(shared_rq_, &recv_wr, &recv_bad_wr) == 0);
}

void GlobalResource::PollWorkCompletion() {
  int send_res = 0, recv_res = 0;
  ibv_wc send_wc, recv_wc;
  memset(&send_wc, 0, sizeof(send_wc));
  memset(&recv_wc, 0, sizeof(recv_wc));
  while (true) {
    if (poller_stop_) return;
    send_res = ibv_poll_cq(shared_send_cq_, 1, &send_wc);
    CHECK(send_res >= 0);
    if (send_res == 1) {
      CHECK(send_wc.status == IBV_WC_SUCCESS);
      ProcessSendWorkCompletion(send_wc);
      memset(&send_wc, 0, sizeof(send_wc));
    }
    recv_res = ibv_poll_cq(shared_recv_cq_, 1, &recv_wc);
    CHECK(recv_res >= 0);
    if (recv_res == 1) {
      CHECK(recv_wc.status == IBV_WC_SUCCESS);
      ProcessRecvWorkCompletion(recv_wc);
      memset(&recv_wc, 0, sizeof(recv_wc));
    }
  }
}

void GlobalResource::ProcessSendWorkCompletion(ibv_wc &wc) {
  uint64_t addr = wc.wr_id;
  if (wc.opcode == IBV_WC_RDMA_WRITE) {
    handler_service_.post([this, addr] {
      ibv_mr *mr_ptr = reinterpret_cast<ibv_mr *>(addr);
      void *mr_addr_ptr = mr_ptr->addr;
      CHECK(ibv_dereg_mr(mr_ptr) == 0);
      mi_free(mr_addr_ptr);
    });
  } else {
    ReturnOneBlock(addr);  // wc.opcode is IBV_WC_SEND
  }
}

int GlobalResource::SelectTargetThread() {
  int goal_thread_idx = 0;
  uint64_t min_remain_cnt = UINT64_MAX;
  for (auto &kv : thread_info_map_) {
    uint64_t temp = kv.second->GetRemainTaskNum();
    if (temp < min_remain_cnt) {
      goal_thread_idx = kv.second->GetVecIndex();
      min_remain_cnt = temp;
    }
  }
  return goal_thread_idx;
}

void GlobalResource::GetAndPostOneBlock() {
  uint64_t block_addr = 0;
  ObtainOneBlock(block_addr);
  PostOneRecvRequest(block_addr);
}

void GlobalResource::GenericSendFunc(uint32_t msg_len,
                                     char *&msg_buf,
                                     const std::function<void()> &serialize_func,
                                     uint32_t rpc_id,
                                     ibv_qp *conn_qp) {
  if (msg_len <= msg_threshold) {
    serialize_func();
    uint64_t block_addr = 0;
    ObtainOneBlock(block_addr);
    memcpy(reinterpret_cast<char *>(block_addr), msg_buf, msg_len);
    SendSmallMessage(conn_qp, block_addr, msg_len, shared_mr_->lkey);
    mi_free(msg_buf);
  } else {
    auto qp_ctx = reinterpret_cast<QueuePairContext *>(conn_qp->qp_context);
    std::promise<RemoteInfo> wait_prom;
    std::future<RemoteInfo> wait_future = wait_prom.get_future();
    {
      std::lock_guard<std::mutex> locker(qp_ctx->rinfo_mtx_);
      qp_ctx->rinfo_map_.insert({rpc_id, wait_prom});
    }
    /// NOTE: For receiving authority message.
    GetAndPostOneBlock();
    // 1. send notify message
    NotifyMessage notify_msg;
    notify_msg.set_rpc_id(rpc_id);
    char notify_buf[max_uint32_field];
    notify_msg.SerializeToArray(notify_buf, max_uint32_field);
    SendControlMessage(conn_qp, notify_buf, max_uint32_field, msg_len);
    // 2. serialize and register memory
    serialize_func();
    ibv_mr *msg_mr = ibv_reg_mr(conn_qp->pd, msg_buf, msg_len, IBV_ACCESS_LOCAL_WRITE);
    CHECK(msg_mr != nullptr);
    // 3. get remote info
    RemoteInfo remote_info = wait_future.get();
    {
      std::lock_guard<std::mutex> locker(qp_ctx->rinfo_mtx_);
      qp_ctx->rinfo_map_.erase(rpc_id);
    }
    // 4. write the large message
    WriteLargeMessage(conn_qp, msg_mr, rpc_id, remote_info);
  }
}

void GlobalResource::ProcessNotifyMessage(uint32_t imm_data, uint64_t recv_addr, ibv_qp *conn_qp) {
  char *msg_addr = reinterpret_cast<char *>(recv_addr);
  auto qp_ctx = reinterpret_cast<QueuePairContext *>(conn_qp->qp_context);
  NotifyMessage notify_msg;
  notify_msg.ParseFromArray(msg_addr, max_uint32_field);  // no check
  uint32_t rpc_id = notify_msg.rpc_id();
  ibv_mr *msg_mr = AllocMemoryRegion(imm_data, conn_qp->pd);
  {
    std::lock_guard<std::mutex> locker(qp_ctx->mr_mtx_);
    qp_ctx->mr_map_.insert({rpc_id, msg_mr});
  }

  /// NOTE: For receiving write-with-imm data (reuse block).
  memset(reinterpret_cast<void *>(recv_addr), 0, msg_threshold);
  PostOneRecvRequest(recv_addr);
  AuthorityMessage authority_msg;
  authority_msg.set_rpc_id(rpc_id);
  authority_msg.set_remote_key(msg_mr->rkey);
  authority_msg.set_remote_addr(reinterpret_cast<uint64_t>(msg_mr->addr));

  uint32_t auth_len = authority_msg.ByteSizeLong();
  AuthorityHead head;
  head.set_authority_size(auth_len);
  uint32_t head_len = max_uint32_field;

  uint32_t total_len = head_len + auth_len;

  char *authority_buf = reinterpret_cast<char *>(mi_malloc(total_len));
  head.SerializeToArray(authority_buf, head_len);
  authority_msg.SerializeToArray(authority_buf + head_len, auth_len);
  SendControlMessage(conn_qp, authority_buf, total_len);
  mi_free(authority_buf);
}

void GlobalResource::ProcessAuthorityMessage(uint64_t recv_addr, ibv_qp *conn_qp) {
  char *msg_addr = reinterpret_cast<char *>(recv_addr);
  auto qp_ctx = reinterpret_cast<QueuePairContext *>(conn_qp->qp_context);

  AuthorityHead head;
  head.ParseFromArray(msg_addr, max_uint32_field);
  uint32_t auth_len = head.authority_size();

  AuthorityMessage authority_msg;
  authority_msg.ParseFromArray(msg_addr + max_uint32_field, auth_len);

  ReturnOneBlock(recv_addr);
  uint32_t rpc_id = authority_msg.rpc_id();
  RemoteInfo rinfo = {.remote_key = authority_msg.remote_key(),
                      .remote_addr = authority_msg.remote_addr()};

  std::lock_guard<std::mutex> locker(qp_ctx->rinfo_mtx_);
  auto it = qp_ctx->rinfo_map_.find(rpc_id);
  CHECK(it != qp_ctx->rinfo_map_.end());
  it->second.set_value(rinfo);
}

ibv_mr *GlobalResource::GetAndEraseMemoryRegion(uint32_t imm_data, ibv_qp *conn_qp) {
  auto qp_ctx = reinterpret_cast<QueuePairContext *>(conn_qp->qp_context);
  // Get and erase the corresponding mr.
  ibv_mr *msg_mr = nullptr;
  std::lock_guard<std::mutex> locker(qp_ctx->mr_mtx_);
  auto it = qp_ctx->mr_map_.find(imm_data);
  CHECK(it != qp_ctx->mr_map_.end());
  msg_mr = it->second;
  qp_ctx->mr_map_.erase(imm_data);
  return msg_mr;
}

}  // namespace lightrpc