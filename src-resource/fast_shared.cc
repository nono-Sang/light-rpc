#include "inc/fast_shared.h"
#include "inc/fast_define.h"
#include "inc/fast_log.h"

static const int min_cqe_num = 512;
static const uint32_t max_srq_wr = 512;
static const uint32_t max_send_wr = 32;
static const uint32_t max_num_sge = 1;
static const int num_cpus = std::thread::hardware_concurrency();

namespace fast {

const uint64_t SharedResource::default_blk_pool_size = msg_threshold * 2048;
const int SharedResource::default_num_work_threads = num_cpus;
const int SharedResource::default_max_local_mr = max_num_cache_mr;
const int SharedResource::default_max_local_block = 512 / num_cpus;

SharedResource::SharedResource(std::string local_ip, 
                               int local_port, 
                               uint64_t blk_pool_size, 
                               int num_work_th, 
                               int max_local_mr, 
                               int max_local_blk)
  : FastResource(local_ip, local_port), 
    block_pool_size_(blk_pool_size), 
    num_work_threads_(num_work_th), 
    max_local_mr_(max_local_mr), 
    max_local_block_(max_local_blk), 
    schedule_idx_(0), 
    addr_queue_(block_pool_size_ / msg_threshold) {
  CreateRDMAResource();
  CreateBlockPool();

  for (int i = 0; i < num_work_threads_; ++i) {
    io_ctx_vec_.emplace_back(std::make_unique<boost::asio::io_context>());
    mr_cache_vec_.emplace_back(std::make_unique<LocalMRCache>(max_local_mr_));
    block_cache_vec_.emplace_back(std::make_unique<ListWithSize>());
    auto& io_ctx = *io_ctx_vec_[i];
    threads_vec_.emplace_back([&io_ctx] {
      boost::asio::io_context::work work(io_ctx);
      io_ctx.run();
    });
    thid_idx_map_.emplace(threads_vec_[i].get_id(), i);
  }

  /// NOTE: Post some recv WRs in advance.
  uint64_t num_blocks = block_pool_size_ / msg_threshold;
  CHECK(num_blocks >= 2 * max_srq_wr);
  for (int i = 0; i < max_srq_wr; i++) {
    uint64_t block_addr = 0;
    GetOneBlockFromGlobalPool(block_addr);
    PostOneRecvRequest(block_addr);
  }
}

SharedResource::~SharedResource() {
  for (int i = 0; i < num_work_threads_; ++i) {
    io_ctx_vec_[i]->stop();
    threads_vec_[i].join();
  }

  void* pool_addr = block_pool_mr_->addr;
  CHECK(ibv_dereg_mr(block_pool_mr_) == 0);
  free(pool_addr);

  CHECK(ibv_destroy_srq(cm_id_->srq) == 0);
  CHECK(ibv_destroy_cq(cm_id_->recv_cq) == 0);
  CHECK(ibv_destroy_comp_channel(cm_id_->recv_cq_channel) == 0);
}

void SharedResource::CreateRDMAResource() {
  ibv_srq_init_attr srq_init_attr;
  memset(&srq_init_attr, 0, sizeof(srq_init_attr));
  /// NOTE: srq_limit is ignored in Infiniband.
  srq_init_attr.attr.max_sge = max_num_sge;
  srq_init_attr.attr.max_wr = max_srq_wr;
  cm_id_->srq = ibv_create_srq(cm_id_->pd, &srq_init_attr);
  CHECK(cm_id_->srq != nullptr);

  cm_id_->recv_cq_channel = ibv_create_comp_channel(cm_id_->verbs);
  CHECK(cm_id_->recv_cq_channel != nullptr);
  cm_id_->recv_cq = ibv_create_cq(
    cm_id_->verbs, min_cqe_num, nullptr, cm_id_->recv_cq_channel, 0);
  CHECK(cm_id_->recv_cq != nullptr);
}

void SharedResource::CreateBlockPool() {
  void* pool_addr = malloc(block_pool_size_);
  CHECK(pool_addr != nullptr);
  block_pool_mr_ = ibv_reg_mr(
    cm_id_->pd, 
    pool_addr, 
    block_pool_size_, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
  );
  CHECK(block_pool_mr_ != nullptr);
  uint64_t num_blocks = block_pool_size_ / msg_threshold;
  uint64_t uint64_addr = reinterpret_cast<uint64_t>(pool_addr);
  for (int i = 0; i < num_blocks; i++) {
    addr_queue_.unsynchronized_push(uint64_addr + i * msg_threshold);
  }
}

void SharedResource::PostOneRecvRequest(uint64_t& block_addr) {
  ibv_sge recv_sg;
  recv_sg.addr = block_addr;
  recv_sg.length = msg_threshold;
  recv_sg.lkey = block_pool_mr_->lkey;

  ibv_recv_wr recv_wr;
  ibv_recv_wr* recv_bad_wr = nullptr;
  memset(&recv_wr, 0, sizeof(recv_wr));
  // Store block address in wr_id field.
  recv_wr.wr_id = block_addr;
  recv_wr.num_sge = 1;
  recv_wr.sg_list = &recv_sg;
  CHECK(ibv_post_srq_recv(cm_id_->srq, &recv_wr, &recv_bad_wr) == 0);
}

void SharedResource::CreateNewQueuePair(rdma_cm_id* conn_id) {
  // Create send cq channel and send cq for conn_id.
  conn_id->send_cq_channel = ibv_create_comp_channel(conn_id->verbs);
  CHECK(conn_id->send_cq_channel != nullptr);
  conn_id->send_cq = ibv_create_cq(
    conn_id->verbs, min_cqe_num, nullptr, conn_id->send_cq_channel, 0);
  CHECK(conn_id->send_cq != nullptr);

  // If sq_sig_all is set to 1, all WRs will generate CQE’s, 
  // else, only WRs that are flagged will generate CQE's.
  ibv_qp_init_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.sq_sig_all = 0;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.srq = cm_id_->srq;          // shared
  qp_attr.send_cq = conn_id->send_cq; // unique
  qp_attr.recv_cq = cm_id_->recv_cq;  // shared
  qp_attr.cap.max_send_wr = max_send_wr;
  qp_attr.cap.max_send_sge = max_num_sge;
  qp_attr.cap.max_inline_data = max_inline_data;
  CHECK(rdma_create_qp(conn_id, conn_id->pd, &qp_attr) == 0);
}

void SharedResource::PutOneBlockIntoLocalCache(int th_idx, uint64_t& block_addr) {
  auto& block_cache = block_cache_vec_.at(th_idx);
  block_cache->list_.push_front(block_addr);
  if (++block_cache->size_ > max_local_block_) {
    uint64_t last_blk_addr = block_cache->list_.back();
    PutOneBlockIntoGlobalPool(last_blk_addr);
    block_cache->list_.pop_back();
    --block_cache->size_;
  }
}

void SharedResource::GetOneBlockFromLocalCache(int th_idx, uint64_t& block_addr) {
  auto& block_cache = block_cache_vec_.at(th_idx);
  if (block_cache->size_ > 0) {
    block_addr = block_cache->list_.front();
    block_cache->list_.pop_front();
    --block_cache->size_;
  } else {
    GetOneBlockFromGlobalPool(block_addr);
  }
}

} // namespace fast