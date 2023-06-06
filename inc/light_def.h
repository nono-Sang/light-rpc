#pragma once

#include <cstdint>

namespace lightrpc {

const int timeout_in_ms = 1000;
const int listen_backlog = 200;

const int min_cqe_num = 512;
const uint32_t srq_max_sge = 1;
const uint32_t srq_max_wr = 512;
const uint32_t max_send_sge = 1;
const uint32_t max_send_wr = 64;

const uint32_t max_inline_data = 256;
const uint32_t msg_threshold = 20 * 1024;

// Max-memory to store uint32 in protobuf.
const int max_uint32_field = 6;

}  // namespace lightrpc