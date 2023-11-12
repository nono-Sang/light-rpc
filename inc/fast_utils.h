#pragma once

#include "inc/fast_log.h"

#include <boost/asio.hpp>
#include <rdma/rdma_cma.h>
#include <mutex>
#include <thread>
#include <list>
#include <vector>
#include <unordered_map>

namespace fast {

class SafeID {
public:
  SafeID(int init): val_(init) {}
  uint32_t GetAndIncOne();

private:
  uint32_t val_;
  std::mutex mtx_;
};


// vType should be pointer type or value type.
template<typename vType>
class SafeHashMap {
public:
  SafeHashMap() = default;
  ~SafeHashMap() = default;
  void SafeInsert(uint32_t key, vType val) {
    std::lock_guard<std::mutex> lk(mtx_);
    hashmap_.emplace(key, val);
  }

  vType SafeGet(uint32_t key) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = hashmap_.find(key);
    CHECK(it != hashmap_.end());
    return it->second;
  }

  void SafeErase(uint32_t key) {
    std::lock_guard<std::mutex> lk(mtx_);
    CHECK(hashmap_.count(key));
    hashmap_.erase(key);
  }

  vType SafeGetAndErase(uint32_t key) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = hashmap_.find(key);
    CHECK(it != hashmap_.end());
    vType val = it->second;
    hashmap_.erase(it);
    return val;
  }

private:
  std::mutex mtx_;
  std::unordered_map<uint32_t, vType> hashmap_;
};


class SafeMRCache {
public:
  SafeMRCache(int max_mr_num);
  ~SafeMRCache();
  ibv_mr* SafeGetAndEraseMR(uint32_t goal_size);
  void SafePutMR(ibv_mr* mr);

private:
  int max_mr_num_;
  int count_;
  std::list<ibv_mr*> mr_list_;
  std::mutex mtx_;
};


class IOContextPool {
public:
  IOContextPool(int num_io_context);
  ~IOContextPool();
  boost::asio::io_context* GetIOContext();

private:
  int num_io_context_;
  std::atomic<uint64_t> curr_index_;
  std::vector<std::thread> threads_vec_;
  std::vector<std::unique_ptr<boost::asio::io_context>> io_ctx_vec_;
};

} // namespace fast