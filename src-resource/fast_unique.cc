#include "inc/fast_unique.h"
#include "inc/fast_define.h"
#include "inc/fast_log.h"

static const int min_cqe_num = 32;
static const uint32_t max_recv_wr = 32;
static const uint32_t max_send_wr = 32;
static const uint32_t max_num_sge = 1;

namespace fast {

const uint64_t UniqueResource::default_blk_pool_size = msg_threshold * 64;

UniqueResource::UniqueResource(std::string local_ip, int local_port, uint64_t blk_pool_size)
  : FastResource(local_ip, local_port, blk_pool_size) {
  this->CreateRDMAResource();
}

UniqueResource::~UniqueResource() {
  CHECK(ibv_destroy_qp(cm_id_->qp) == 0);
  CHECK(ibv_destroy_cq(cm_id_->recv_cq) == 0);
  CHECK(ibv_destroy_cq(cm_id_->send_cq) == 0);
  CHECK(ibv_destroy_comp_channel(cm_id_->recv_cq_channel) == 0);
  CHECK(ibv_destroy_comp_channel(cm_id_->send_cq_channel) == 0);
}

void UniqueResource::PostOneRecvRequest(uint64_t& block_addr) {
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
  CHECK(ibv_post_recv(cm_id_->qp, &recv_wr, &recv_bad_wr) == 0);
}

void UniqueResource::CreateRDMAResource() {
  cm_id_->recv_cq_channel = ibv_create_comp_channel(cm_id_->verbs);
  CHECK(cm_id_->recv_cq_channel != nullptr);
  cm_id_->send_cq_channel = ibv_create_comp_channel(cm_id_->verbs);
  CHECK(cm_id_->send_cq_channel != nullptr);
  cm_id_->recv_cq = ibv_create_cq(
    cm_id_->verbs, min_cqe_num, nullptr, cm_id_->recv_cq_channel, 0);
  CHECK(cm_id_->recv_cq != nullptr);
  cm_id_->send_cq = ibv_create_cq(
    cm_id_->verbs, min_cqe_num, nullptr, cm_id_->send_cq_channel, 0);
  CHECK(cm_id_->send_cq != nullptr);

  // If sq_sig_all is set to 1, all WRs will generate CQE’s, 
  // else, only WRs that are flagged will generate CQE's.
  ibv_qp_init_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.sq_sig_all = 0;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.recv_cq = cm_id_->recv_cq;
  qp_attr.send_cq = cm_id_->send_cq;
  qp_attr.cap.max_recv_wr = max_recv_wr;
  qp_attr.cap.max_recv_sge = max_num_sge;
  qp_attr.cap.max_send_wr = max_send_wr;
  qp_attr.cap.max_send_sge = max_num_sge;
  qp_attr.cap.max_inline_data = max_inline_data;
  CHECK(rdma_create_qp(cm_id_, cm_id_->pd, &qp_attr) == 0);
}

} // namespace fast