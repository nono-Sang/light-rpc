#pragma once

#include <cstdint>

namespace fast {

extern const uint32_t msg_threshold;
extern const uint32_t max_inline_data;

extern const uint32_t fixed32_bytes;
extern const uint32_t fixed_noti_bytes;
extern const uint32_t fixed_auth_bytes;
extern const uint32_t fixed_rep_head_bytes;

extern const int max_num_cache_mr;

extern const int timeout_in_ms;
extern const int listen_backlog;

enum MessageType {
  FAST_SmallMessage = 1, 
  FAST_NotifyMessage = 2
};

} // namespace fast