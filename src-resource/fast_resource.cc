#include "inc/fast_resource.h"
#include "inc/fast_define.h"
#include "inc/fast_log.h"

#include <arpa/inet.h>

namespace fast {

FastResource::FastResource(std::string local_ip, int local_port, uint64_t blk_pool_size)
  : local_ip_(local_ip), 
    local_port_(local_port), 
    block_pool_size_(blk_pool_size), 
    addr_queue_(block_pool_size_ / msg_threshold) {
  BindToLocalRNIC();
  CreateBlockPool();
}

FastResource::~FastResource() {
  void* pool_addr = block_pool_mr_->addr;
  CHECK(ibv_dereg_mr(block_pool_mr_) == 0);
  free(pool_addr);
  auto temp_chan = cm_id_->channel;
  CHECK(rdma_destroy_id(cm_id_) == 0);
  rdma_destroy_event_channel(temp_chan);
}

void FastResource::BindToLocalRNIC() {
  auto temp_channel = rdma_create_event_channel();
  CHECK(temp_channel != nullptr);
  CHECK(rdma_create_id(temp_channel, &cm_id_, nullptr, RDMA_PS_TCP) == 0);

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(local_ip_.c_str());
  addr.sin_port = htons(local_port_);
  CHECK(rdma_bind_addr(cm_id_, reinterpret_cast<sockaddr*>(&addr)) == 0);
}

void FastResource::CreateBlockPool() {
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

void FastResource::ObtainOneBlock(uint64_t& addr) {
  // Set non-blocking to blocking.
  if (!addr_queue_.pop(addr)) {
    LOG_INFO("## Waiting for a idle blocks.");
    while (!addr_queue_.pop(addr)) {};
    LOG_INFO("## Has gotten an idle block.");
  }
}

ibv_mr* FastResource::AllocAndRegisterMR(uint32_t buf_size) {
  void* buf = malloc(buf_size);
  CHECK(buf != nullptr);
  ibv_mr* ans_mr = ibv_reg_mr(
    cm_id_->pd, 
    buf, 
    buf_size, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
  );
  CHECK(ans_mr != nullptr);
  return ans_mr;
}
  
} // namespace fast