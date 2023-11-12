#pragma once

#include "inc/fast_resource.h"

#include <rdma/rdma_cma.h>
#include <boost/lockfree/queue.hpp>

namespace fast {

class SharedResource : public FastResource {
public:
  SharedResource(std::string local_ip,
                 int local_port, 
                 uint64_t blk_pool_size = default_blk_pool_size);
  virtual ~SharedResource();
  void CreateQueuePair(rdma_cm_id* conn_id);
  virtual void PostOneRecvRequest(uint64_t& block_addr) override;

private:
  virtual void CreateRDMAResource() override;
  static const uint64_t default_blk_pool_size;
};

} // namespace fast