#pragma once

#include <rdma/rdma_cma.h>
#include <boost/lockfree/queue.hpp>

namespace fast {

class FastResource {
public:
  FastResource(std::string local_ip, int local_port, uint64_t blk_pool_size);
  virtual ~FastResource();

  void ObtainOneBlock(uint64_t& addr);
  ibv_mr* AllocAndRegisterMR(uint32_t buf_size);
  virtual void PostOneRecvRequest(uint64_t& block_addr) = 0;  // pure virtual function

protected:
  virtual void CreateRDMAResource() = 0;  // pure virtual function

  // unique resource: QP, SEND/RECV CQ, SEND/RECV CQ channel
  // shared resource: SRQ, Common RECV CQ, Common RECV CQ channel
  rdma_cm_id* cm_id_;
  std::string local_ip_;
  int local_port_;
  uint64_t block_pool_size_;
  boost::lockfree::queue<uint64_t> addr_queue_;
  ibv_mr* block_pool_mr_;

private:
  void BindToLocalRNIC();
  void CreateBlockPool();

public:
  inline uint32_t GetLocalKey() const {
    return block_pool_mr_->lkey;
  }
  inline uint32_t GetRemoteKey() const {
    return block_pool_mr_->rkey;
  }
  inline int GetLocalPort() const {
    return local_port_;
  }
  inline ibv_cq* GetRecvCQ() const {
    return cm_id_->recv_cq;
  }
  inline rdma_cm_id* GetConnMgrID() const {
    return cm_id_;
  }
  inline void ReturnOneBlock(uint64_t& addr) {
    addr_queue_.push(addr);
  }
};

} // namespace fast