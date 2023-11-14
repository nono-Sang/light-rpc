#include "inc/fast_define.h"
#include "build/fast_impl.pb.h"

namespace fast {

static uint32_t GetFixed32Bytes() {
  Fixed32Bytes obj;
  obj.set_val(1024);
  return obj.ByteSizeLong();
}

static uint32_t GetFixedNotiBytes() {
  NotifyMessage obj;
  obj.set_total_len(1024);
  obj.set_remote_key(1024);
  obj.set_remote_addr(1024);
  return obj.ByteSizeLong();
}

static uint32_t GetFixedAuthBytes() {
  AuthorityMessage obj;
  obj.set_remote_key(1024);
  obj.set_remote_addr(1024);
  return obj.ByteSizeLong();
}

static uint32_t GetFixedRepHeadBytes() {
  ResponseHead obj;
  obj.set_rpc_id(1024);
  obj.set_total_len(1024);
  return obj.ByteSizeLong();
}

const uint32_t msg_threshold = 6 * 1024;
const uint32_t max_inline_data = 200;

const uint32_t fixed32_bytes = GetFixed32Bytes();
const uint32_t fixed_noti_bytes = GetFixedNotiBytes();
const uint32_t fixed_auth_bytes = GetFixedAuthBytes();
const uint32_t fixed_rep_head_bytes = GetFixedRepHeadBytes();

const int max_num_cache_mr = 5;

const int timeout_in_ms = 1000;
const int listen_backlog = 200;

} // namespace fast