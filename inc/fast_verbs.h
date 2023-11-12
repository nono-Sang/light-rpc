#pragma once

#include "inc/fast_define.h"

#include <rdma/rdma_cma.h>

namespace fast {

/// @brief Send a message using inline mode (generate a CQE).
/// This function sets wr_id to 0.
/// @param qp 
/// @param msg_type This function sets imm_data to msg_type.
/// @param msg_addr 
/// @param msg_len 
void SendInlineMessage(ibv_qp* qp, 
                       MessageType msg_type, 
                       uint64_t msg_addr, 
                       uint32_t msg_len);

/// @brief Send a small message (generate a CQE).
/// @param qp 
/// @param msg_type This function sets imm_data to msg_type.
/// @param msg_addr This function sets wr_id to msg_addr (block address).
/// @param msg_len 
/// @param lkey 
void SendSmallMessage(ibv_qp* qp, 
                      MessageType msg_type, 
                      uint64_t msg_addr, 
                      uint32_t msg_len, 
                      uint32_t lkey);

/// @brief Use the write operation and inline mode to send a message (generate a CQE).
/// This function sets wr_id to 0.
/// @param qp 
/// @param msg_addr 
/// @param msg_len 
/// @param remote_key 
/// @param remote_addr 
void WriteInlineMessage(ibv_qp* qp, 
                        uint64_t msg_addr, 
                        uint32_t msg_len, 
                        uint32_t remote_key, 
                        uint64_t remote_addr);

/// @brief Use the write operation to send a large message (generate a CQE).
/// @param qp
/// @param msg_mr This function sets wr_id to the MR address.
/// @param msg_len The length of MR may be greater than the msg_len.
/// @param remote_key 
/// @param remote_addr 
void WriteLargeMessage(ibv_qp* qp,
                       ibv_mr* msg_mr, 
                       uint32_t msg_len, 
                       uint32_t remote_key, 
                       uint64_t remote_addr);
  
} // namespace fast