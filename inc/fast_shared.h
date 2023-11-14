#pragma once

#include "inc/fast_resource.h"
#include "inc/fast_utils.h"

#include <rdma/rdma_cma.h>
#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>

namespace fast {

struct ListWithSize {
  int size_;
  std::list<uint64_t> list_;
  ListWithSize() : size_(0) {}
};

class SharedResource : public FastResource {
public:
  SharedResource(std::string local_ip, int local_port, 
                 uint64_t blk_pool_size = default_blk_pool_size, 
                 int num_work_th = default_num_work_threads, 
                 int max_local_mr = default_max_local_mr, 
                 int max_local_blk = default_max_local_block);
  virtual ~SharedResource();

  void PostOneRecvRequest(uint64_t& block_addr);
  void CreateNewQueuePair(rdma_cm_id* conn_id);
  void PutOneBlockIntoLocalCache(int th_idx, uint64_t& block_addr);
  void GetOneBlockFromLocalCache(int th_idx, uint64_t& block_addr);
  void PutOneMRIntoCache(int th_idx, ibv_mr* mr);
  ibv_mr* GetOneMRFromCache(int th_idx, uint32_t goal_size);

  boost::asio::io_context* GetIOContext();
  int GetThreadIndex(std::thread::id th_id);
  void PutOneBlockIntoGlobalPool(uint64_t& block_addr);
  void GetOneBlockFromGlobalPool(uint64_t& block_addr);
  uint32_t GetLocalKey() const;
  uint32_t GetRemoteKey() const;

private:
  virtual void CreateRDMAResource() override;
  void CreateBlockPool();

  uint64_t block_pool_size_;
  int num_work_threads_;
  int max_local_mr_;
  int max_local_block_;
  std::atomic<uint64_t> schedule_idx_;

  boost::lockfree::queue<uint64_t> addr_queue_;
  ibv_mr* block_pool_mr_;

  std::vector<std::thread> threads_vec_;
  std::unordered_map<std::thread::id, int> thid_idx_map_;
  std::vector<std::unique_ptr<boost::asio::io_context>> io_ctx_vec_;
  std::vector<std::unique_ptr<LocalMRCache>> mr_cache_vec_;
  std::vector<std::unique_ptr<ListWithSize>> block_cache_vec_;

  static const uint64_t default_blk_pool_size;
  static const int default_num_work_threads;
  static const int default_max_local_mr;
  static const int default_max_local_block;
};

inline boost::asio::io_context* SharedResource::GetIOContext() {
  int idx = ++schedule_idx_ % num_work_threads_;
  return io_ctx_vec_.at(idx).get();
}

inline int SharedResource::GetThreadIndex(std::thread::id th_id) {
  return thid_idx_map_.at(th_id);
}

inline void SharedResource::PutOneBlockIntoGlobalPool(uint64_t& block_addr) {
  addr_queue_.push(block_addr);
}

inline void SharedResource::GetOneBlockFromGlobalPool(uint64_t& block_addr) {
  if (!addr_queue_.pop(block_addr)) {
    LOG_INFO("## Waiting for idle blocks.");
    while (!addr_queue_.pop(block_addr)) {}
    LOG_INFO("## Has gotten an idle block.");
  }
}

inline void SharedResource::PutOneMRIntoCache(int th_idx, ibv_mr* mr) {
  auto& mr_cache = mr_cache_vec_.at(th_idx);
  mr_cache->PushOneMRIntoCache(mr);
}

inline ibv_mr* SharedResource::GetOneMRFromCache(int th_idx, uint32_t goal_size) {
  auto& mr_cache = mr_cache_vec_.at(th_idx);
  return mr_cache->GetOneMRFromCache(goal_size);
}

inline uint32_t SharedResource::GetLocalKey() const { 
  return block_pool_mr_->lkey; 
}
  
inline uint32_t SharedResource::GetRemoteKey() const { 
  return block_pool_mr_->rkey; 
}

} // namespace fast