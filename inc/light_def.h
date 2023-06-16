#pragma once

#include <cstdint>

namespace lightrpc {

const int timeout_in_ms = 1000;
const int listen_backlog = 200;

// The maximum length is 2^32-1=4294967296,
// which needs 11 digits (includes '\0').
const int max_length_digit = 11;

const int min_cqe_num = 1024;
const uint32_t srq_max_sge = 1;
const uint32_t srq_max_wr = 1024;
const uint32_t max_send_sge = 1;
const uint32_t max_send_wr = 256;

const uint32_t max_inline_data = 256;
const uint32_t msg_threshold = 20 * 1024;

}  // namespace lightrpc