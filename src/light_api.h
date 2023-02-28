#pragma once

#include <rdma/rdma_cma.h>
#include "src/light_log.h"

namespace lightrpc {

struct RemoteInfo {
    uint32_t remote_key;
    uint64_t remote_addr;
};

struct RemoteInfoWithId {
    uint32_t rpc_id;
    uint32_t remote_key;
    uint64_t remote_addr;
};

/// Send the message in the block and set imm_data to
/// msg_len. This func sets wr_id to block addr. User can
/// return the block by polling send work completion.
void
SendSmallMessage(ibv_qp* qp, uint64_t block_addr, uint32_t msg_len,
                 uint32_t lkey);

/// For large message, send a notify message. This func
/// sets imm_data to msg_len and use inline mode. If
/// msg_len >= max_threshold, T is RemoteInfoWithId type.
/// Otherwise, T is RemoteInfo type.
template <typename T>
void
SendNotifyMessage(ibv_qp* qp, uint32_t msg_len, T& info) {
    ibv_sge send_sg = {.addr = reinterpret_cast<uint64_t>(&info),
                       .length = sizeof(info)};

    ibv_send_wr send_wr;
    ibv_send_wr* send_bad_wr = nullptr;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.num_sge = 1;
    send_wr.sg_list = &send_sg;
    send_wr.imm_data = htonl(msg_len);
    send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = IBV_SEND_INLINE;
    CHECK(ibv_post_send(qp, &send_wr, &send_bad_wr) == 0);
}

/// Write the rkey and addr of msg_mr to target remote.
/// Note that the message body contains a tag byte. This
/// func use inline mode and ignore imm_data field.
void
WriteMemoryRegionInfo(ibv_qp* qp, ibv_mr* msg_mr, RemoteInfo& target);

/// Write the large message (in msg_mr) to remote. If
/// length >= max_threshold, this func set imm_data to
/// rpc_id. Otherwise, user should set rpc_id to 0.
void
WriteLargeMessage(ibv_qp* qp, ibv_mr* msg_mr, uint32_t rpc_id,
                  RemoteInfo& target);

void
QueryDeviceAttribute(ibv_context* ctx);

} // namespace lightrpc