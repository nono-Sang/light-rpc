#pragma once

#include <mutex>
#include <shared_mutex>

namespace lightrpc {

enum PollingMode {
  EVENT_NOTIFY,
  BUSY_POLLING
};

/// If the local_port is set to a negative number, 
/// the system will automatically allocate a port.
struct ResourceConfig {
  std::string local_ip;
  int local_port;
  int num_threads;
  PollingMode poll_mode;
  int block_pool_size;
};

/**
 * Important parameters:
 * srq_max_wr: 512
 * min_cqe_num: 512
 * max_send_wr: 64
 * msg_threshold: 6*1024
 * max_threshold: 1024*1024
 */
const uint32_t srq_max_sge = 1;
const uint32_t srq_max_wr = 512;
const int min_cqe_num = 512;
const uint32_t max_send_sge = 1;
const uint32_t max_send_wr = 64;
const uint32_t max_inline_data = 256;
const uint32_t msg_threshold = 6 * 1024;
const uint32_t max_threshold = 1024 * 1024;

using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::unique_lock<std::shared_mutex>;

/// Used to record the size of the metadata.
const int req_fixed_len = sizeof(uint16_t);
const int max_metadata_size = UINT16_MAX;
/// Used to record the corresponding rpc id.
const int res_head_len = sizeof(uint32_t);
  
} // namespace lightrpc