#pragma once

#include "inc/fast_resource.h"

#include <rdma/rdma_cma.h>
#include <boost/lockfree/queue.hpp>

namespace fast {

class UniqueResource : public FastResource {
public:
  UniqueResource(std::string local_ip,
                 int local_port = 0, 
                 uint64_t blk_pool_size = default_blk_pool_size);
  virtual ~UniqueResource();
  virtual void PostOneRecvRequest(uint64_t& block_addr) override;

public:
  inline ibv_cq* GetSendCQ() const {
    return cm_id_->send_cq;
  }
  inline ibv_qp* GetQueuePair() const {
    return cm_id_->qp;
  }

private:
  virtual void CreateRDMAResource() override;
  static const uint64_t default_blk_pool_size;
};

} // namespace fast