#include "inc/fast_utils.h"

namespace fast {

uint32_t SafeID::GetAndIncOne() {
  std::lock_guard<std::mutex> lk(mtx_);
  int curr = val_;
  ++val_;
  return curr;
}

SafeMRCache::SafeMRCache(int max_mr_num): max_mr_num_(max_mr_num), count_(0) {}

SafeMRCache::~SafeMRCache() {
  for (auto it = mr_list_.begin(); it != mr_list_.end(); ++it) {
    void* addr = (*it)->addr;
    CHECK(ibv_dereg_mr(*it) == 0);
    free(addr);
  }
}

ibv_mr* SafeMRCache::SafeGetAndEraseMR(uint32_t goal_size) {
  ibv_mr* ans = nullptr;
  std::lock_guard<std::mutex> lk(mtx_);
  for (auto it = mr_list_.begin(); it != mr_list_.end(); ++it) {
    if ((*it)->length >= goal_size) {
      ans = *it;
      // Remove it from the list.
      --count_;
      mr_list_.erase(it);
      break;
    }
  }
  return ans;
}

void SafeMRCache::SafePutMR(ibv_mr* new_mr) {
  std::lock_guard<std::mutex> lk(mtx_);
  mr_list_.push_front(new_mr);
  ++count_;
  if (count_ > max_mr_num_) {
    auto elem = mr_list_.back();
    void* addr = elem->addr;
    CHECK(ibv_dereg_mr(elem) == 0);
    free(addr);
    mr_list_.pop_back();
    --count_;
  }
}

IOContextPool::IOContextPool(int num_io_context)
  : num_io_context_(num_io_context), curr_index_(0) {
  for (int i = 0; i < num_io_context_; ++i) {
    io_ctx_vec_.emplace_back(std::make_unique<boost::asio::io_context>());
    auto& ctx_ref = *io_ctx_vec_[i];
    threads_vec_.emplace_back([&ctx_ref] {
      boost::asio::io_context::work work(ctx_ref);
      ctx_ref.run();
    });
  }
}

IOContextPool::~IOContextPool() {
  for (int i = 0; i < num_io_context_; ++i) {
    io_ctx_vec_[i]->stop();
    threads_vec_[i].join();
  }
}

boost::asio::io_context* IOContextPool::GetIOContext() {
  uint64_t index = ++curr_index_ % num_io_context_;
  return io_ctx_vec_[index].get();
}

} // namespace fast