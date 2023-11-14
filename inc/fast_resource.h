#pragma once

#include <rdma/rdma_cma.h>
#include <string>

namespace fast {

class FastResource {
public:
  FastResource(std::string local_ip, int local_port);
  virtual ~FastResource();
  ibv_mr* AllocAndRegisterMR(uint32_t buf_size);
  virtual void CreateRDMAResource() = 0;  // pure virtual function

  inline rdma_cm_id* GetConnMgrID() const { return cm_id_; }
  inline int GetLocalPort() const { return local_port_; }

protected:
  // common verbs and pd
  rdma_cm_id* cm_id_;
  std::string local_ip_;
  int local_port_;

private:
  void BindToLocalRNIC();
};

} // namespace fast