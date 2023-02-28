#include "src/light_api.h"

#include <mimalloc/mimalloc.h>

namespace lightrpc {

void SendSmallMessage(ibv_qp* qp, uint64_t block_addr,
                      uint32_t msg_len, uint32_t lkey) {
  ibv_sge send_sg = {
    .addr = block_addr,
    .length = msg_len,
    .lkey = lkey
  };

  ibv_send_wr send_wr;
  ibv_send_wr* send_bad_wr = nullptr;
  memset(&send_wr, 0, sizeof(send_wr));
  // Set wr_id to block address.
  send_wr.wr_id = block_addr;
  send_wr.num_sge = 1;
  send_wr.sg_list = &send_sg;
  send_wr.imm_data = htonl(msg_len);
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  CHECK(ibv_post_send(qp, &send_wr, &send_bad_wr) == 0);
}

void WriteMemoryRegionInfo(ibv_qp* qp, ibv_mr* msg_mr, 
                           RemoteInfo& target) {
  void* info_buf = mi_malloc(sizeof(RemoteInfo) + 1);
  CHECK(info_buf != nullptr);
  RemoteInfo remote_info = {
    .remote_key = msg_mr->rkey,
    .remote_addr = reinterpret_cast<uint64_t>(msg_mr->addr)
  };
  memcpy(info_buf, &remote_info, sizeof(remote_info));
  memset(reinterpret_cast<char*>(
    info_buf) + sizeof(remote_info), '1', 1);
  ibv_sge write_sg = {
    .addr = reinterpret_cast<uint64_t>(info_buf),
    .length = sizeof(remote_info) + 1
  };

  ibv_send_wr write_wr;
  ibv_send_wr* write_bad_wr = nullptr;
  memset(&write_wr, 0, sizeof(write_wr));
  write_wr.num_sge = 1;
  write_wr.sg_list = &write_sg;
  write_wr.opcode = IBV_WR_RDMA_WRITE;
  write_wr.send_flags = IBV_SEND_INLINE;
  write_wr.wr.rdma.rkey = target.remote_key;
  write_wr.wr.rdma.remote_addr = target.remote_addr;
  CHECK(ibv_post_send(qp, &write_wr, &write_bad_wr) == 0);
  mi_free(info_buf);
}

void WriteLargeMessage(ibv_qp* qp, ibv_mr* msg_mr, 
                       uint32_t rpc_id, RemoteInfo& target) {
  ibv_sge write_sg = {
    .addr = reinterpret_cast<uint64_t>(msg_mr->addr),
    .length = static_cast<uint32_t>(msg_mr->length),
    .lkey = msg_mr->lkey
  };

  ibv_send_wr write_wr;
  ibv_send_wr* write_bad_wr = nullptr;
  memset(&write_wr, 0, sizeof(write_wr));
  // Release the mr and addr by polling send wc.
  write_wr.wr_id = reinterpret_cast<uint64_t>(msg_mr);
  write_wr.num_sge = 1;
  write_wr.sg_list = &write_sg;
  if (rpc_id > 0) {
    write_wr.imm_data = htonl(rpc_id);
    write_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  } else {
    write_wr.opcode = IBV_WR_RDMA_WRITE;
  }
  write_wr.send_flags = IBV_SEND_SIGNALED;
  write_wr.wr.rdma.rkey = target.remote_key;
  write_wr.wr.rdma.remote_addr = target.remote_addr;
  CHECK(ibv_post_send(qp, &write_wr, &write_bad_wr) == 0);
}

void QueryDeviceAttribute(ibv_context* ctx) {
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
  
} // namespace lightrpc