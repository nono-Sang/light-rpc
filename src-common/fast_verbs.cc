#include "inc/fast_verbs.h"
#include "inc/fast_log.h"

namespace fast {

void SendInlineMessage(ibv_qp* qp, 
                       MessageType msg_type, 
                       uint64_t msg_addr, 
                       uint32_t msg_len) {
  ibv_sge send_sg;
  send_sg.addr = msg_addr;
  send_sg.length = msg_len;

  ibv_send_wr send_wr;
  ibv_send_wr* send_bad_wr = nullptr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.wr_id = 0;  // 0
  send_wr.num_sge = 1;
  send_wr.sg_list = &send_sg;
  send_wr.imm_data = htonl(msg_type);
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_SOLICITED | IBV_SEND_INLINE;  // gen CQE
  CHECK(ibv_post_send(qp, &send_wr, &send_bad_wr) == 0);
}

void SendSmallMessage(ibv_qp* qp, 
                      MessageType msg_type, 
                      uint64_t msg_addr, 
                      uint32_t msg_len, 
                      uint32_t lkey) {
  ibv_sge send_sg;
  send_sg.addr = msg_addr;
  send_sg.length = msg_len;
  send_sg.lkey = lkey;

  ibv_send_wr send_wr;
  ibv_send_wr* send_bad_wr = nullptr;
  memset(&send_wr, 0, sizeof(send_wr));
  // Set wr_id to block address.
  send_wr.wr_id = msg_addr;
  send_wr.num_sge = 1;
  send_wr.sg_list = &send_sg;
  send_wr.imm_data = htonl(msg_type);
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_SOLICITED;  // gen CQE
  CHECK(ibv_post_send(qp, &send_wr, &send_bad_wr) == 0);       
}

void WriteInlineMessage(ibv_qp* qp, 
                        uint64_t msg_addr, 
                        uint32_t msg_len, 
                        uint32_t remote_key, 
                        uint64_t remote_addr) {
  ibv_sge write_sg;
  write_sg.addr = msg_addr;
  write_sg.length = msg_len;
  
  ibv_send_wr write_wr;
  ibv_send_wr* write_bad_wr = nullptr;
  memset(&write_wr, 0, sizeof(write_wr));
  write_wr.wr_id = 0;  // 0
  write_wr.num_sge = 1;
  write_wr.sg_list = &write_sg;
  write_wr.opcode = IBV_WR_RDMA_WRITE;
  write_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;  // gen CQE
  write_wr.wr.rdma.rkey = remote_key;
  write_wr.wr.rdma.remote_addr = remote_addr;
  CHECK(ibv_post_send(qp, &write_wr, &write_bad_wr) == 0);
}

void WriteLargeMessage(ibv_qp* qp,  
                       ibv_mr* msg_mr, 
                       uint32_t msg_len, 
                       uint32_t remote_key, 
                       uint64_t remote_addr) {
  ibv_sge write_sg;
  write_sg.addr = reinterpret_cast<uint64_t>(msg_mr->addr);
  write_sg.length = msg_len;  // important!
  write_sg.lkey = msg_mr->lkey;

  ibv_send_wr write_wr;
  ibv_send_wr* write_bad_wr = nullptr;
  memset(&write_wr, 0, sizeof(write_wr));
  // Set wr_id to MR address.
  write_wr.wr_id = reinterpret_cast<uint64_t>(msg_mr);
  write_wr.num_sge = 1;
  write_wr.sg_list = &write_sg;
  write_wr.opcode = IBV_WR_RDMA_WRITE;
  write_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_SOLICITED;  // gen CQE
  write_wr.wr.rdma.rkey = remote_key;
  write_wr.wr.rdma.remote_addr = remote_addr;
  CHECK(ibv_post_send(qp, &write_wr, &write_bad_wr) == 0);
}

} // namespace fast