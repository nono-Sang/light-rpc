#pragma once

#include "inc/fast_resource.h"
#include "inc/fast_utils.h"

#include <rdma/rdma_cma.h>
#include <memory>

namespace fast {

class UniqueResource : public FastResource {
public:
  UniqueResource(std::string local_ip, int local_port = 0, 
                 int blk_pool_size = default_blk_pool_size, 
                 int max_local_mr = default_max_local_mr);
  virtual ~UniqueResource();

  void PostOneRecvRequest(uint64_t& block_addr);
  
  void ObtainOneBlock(uint64_t& block_addr);
  void ReturnOneBlock(uint64_t& block_addr);
  void PutOneMRIntoCache(ibv_mr* mr);
  ibv_mr* GetOneMRFromCache(uint32_t goal_size);
  uint32_t GetLocalKey() const;
  uint32_t GetRemoteKey() const;

private:
  virtual void CreateRDMAResource() override;
  void CreateBlockPool();

  uint64_t block_pool_size_;
  int max_local_mr_;

  ibv_mr* block_pool_mr_;
  std::list<uint64_t> block_list_;
  std::unique_ptr<LocalMRCache> large_mr_cache_;

  static const uint64_t default_blk_pool_size;
  static const int default_max_local_mr;
};

inline void UniqueResource::ObtainOneBlock(uint64_t& block_addr) {
  block_addr = block_list_.front();
  block_list_.pop_front();
}

inline void UniqueResource::ReturnOneBlock(uint64_t& block_addr) {
  block_list_.push_front(block_addr);
}

inline void UniqueResource::PutOneMRIntoCache(ibv_mr* mr) {
  large_mr_cache_->PushOneMRIntoCache(mr);
}

inline ibv_mr* UniqueResource::GetOneMRFromCache(uint32_t goal_size) {
  return large_mr_cache_->GetOneMRFromCache(goal_size);
}

inline uint32_t UniqueResource::GetLocalKey() const { 
  return block_pool_mr_->lkey; 
}
  
inline uint32_t UniqueResource::GetRemoteKey() const { 
  return block_pool_mr_->rkey; 
}

} // namespace fast