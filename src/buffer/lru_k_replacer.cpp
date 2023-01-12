//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

// 1. store iterator is ok? ok
// 2. no need to maintain cache_queue_ freq? yes
// 3. return is suitable to abort the process? yes
// 4. initial evictable is true or false? false(to pass the concurrency test)
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : capacity_(num_frames), replacer_size_(0), k_(k) {
  is_evictable_.resize(capacity_, -1);
  freq_.resize(capacity_, 0);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (replacer_size_ == 0) {
    return false;
  }

  // if history_queue_ or cache_queue_ is evictable
  // front is set to evict, however, not always evictable
  --replacer_size_;
  for (auto it = history_queue_.begin(); it != history_queue_.end(); ++it) {
    int id = *it;
    if (is_evictable_[id] > 0) {
      // maintain the field
      *frame_id = id;
      is_evictable_[id] = -1;
      history_map_.erase(id);
      history_queue_.erase(it);
      freq_[id] = 0;
      return true;
    }
  }
  for (auto it = cache_queue_.begin(); it != cache_queue_.end(); ++it) {
    auto id = *it;
    if (is_evictable_[id] > 0) {
      // maintain the field
      *frame_id = id;
      is_evictable_[id] = -1;
      cache_map_.erase(id);
      cache_queue_.erase(it);
      freq_[id] = 0;
      break;
    }
  }

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // if invalid, return;
  //  ++freq
  // if freq is k_, which means id is in history queue and need to move to cache_queue_
  // else if in cache_queue_, adjust the order(asc)
  // else if new one , emplace_back new one in history_queue_
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id >= static_cast<frame_id_t>(capacity_)) {
    return;
  }

  size_t freq = ++freq_[frame_id];
  if (freq == k_) {
    history_queue_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
    cache_queue_.emplace_back(frame_id);
    cache_map_[frame_id] = --cache_queue_.end();
  } else if (freq > k_) {
    auto it = cache_map_[frame_id];
    cache_queue_.erase(it);
    cache_queue_.emplace_back(frame_id);
    cache_map_[frame_id] = --cache_queue_.end();
  } else if (freq == 1) {
    history_queue_.emplace_back(frame_id);
    history_map_[frame_id] = --history_queue_.end();
    is_evictable_[frame_id] = 0;
    // ++replacer_size_;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (is_evictable_[frame_id] == -1) {
    return;
  }

  int set_flag = static_cast<int>(set_evictable);
  if (is_evictable_[frame_id] != set_flag) {
    replacer_size_ += (set_flag == 1 ? 1 : -1);
    is_evictable_[frame_id] = set_flag;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // if id not exists or not evictable, return false
  size_t freq = freq_[frame_id];
  if (is_evictable_[frame_id] == -1 || is_evictable_[frame_id] == 0) {
    return;
  }

  // else, maintain the field
  --replacer_size_;
  is_evictable_[frame_id] = -1;
  freq_[frame_id] = 0;
  if (freq < k_) {
    auto remove_it = history_map_[frame_id];
    history_map_.erase(frame_id);
    history_queue_.erase(remove_it);
  } else {
    auto remove_it = cache_map_[frame_id];
    cache_map_.erase(frame_id);
    cache_queue_.erase(remove_it);
  }
}
auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

}  // namespace bustub
