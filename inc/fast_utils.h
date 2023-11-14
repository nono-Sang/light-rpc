#pragma once

#include "inc/fast_log.h"

#include <rdma/rdma_cma.h>
#include <list>
#include <unordered_map>
#include <mutex>

namespace fast {

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


class LocalMRCache {
public:
  LocalMRCache(int capacity): capacity_(capacity), size_(0) {}

  ~LocalMRCache() {
    for (auto it = mr_list_.begin(); it != mr_list_.end(); ++it) {
      void* mr_addr = (*it)->addr;
      CHECK(ibv_dereg_mr(*it) == 0);
      free(mr_addr);
    }
  }

  void PushOneMRIntoCache(ibv_mr* mr) {
    mr_list_.push_front(mr);
    if (++size_ > capacity_) {
      ibv_mr* last_mr = mr_list_.back();
      void* last_mr_addr = last_mr->addr;
      CHECK(ibv_dereg_mr(last_mr) == 0);
      free(last_mr_addr);
      mr_list_.pop_back();
      --size_;
    }
  }

  ibv_mr* GetOneMRFromCache(uint32_t goal_size) {
    ibv_mr* ans = nullptr;
    for (auto it = mr_list_.begin(); it != mr_list_.end(); ++it) {
      if ((*it)->length >= goal_size) {
        ans = *it;
        mr_list_.erase(it);
        --size_;
        break;
      }
    }
    return ans;
  }

private:
  int capacity_;
  int size_;
  std::list<ibv_mr*> mr_list_;
};

} // namespace fast