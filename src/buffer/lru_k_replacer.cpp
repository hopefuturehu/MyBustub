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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto iter = history_.rbegin(); iter != history_.rend(); iter++) {
    if (non_evictale_[iter->first]) {
      non_evictale_.erase(iter->first);
      *frame_id = iter->first;
      history_map_.erase(iter->first);
      history_.remove(*iter);
      curr_size_--;
      return true;
    }
  }
  for (auto iter = buffer_.rbegin(); iter != buffer_.rend(); iter++) {
    if (non_evictale_[iter->first]) {
      *frame_id = iter->first;
      non_evictale_.erase((iter->first));
      buffer_map_.erase(iter->first);
      buffer_.remove(*iter);
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::logic_error(std::string("Invalid framed_id in RA") + std::to_string(frame_id));
  }
  current_timestamp_++;
  auto iter = buffer_map_.find(frame_id);
  if (iter != buffer_map_.end()) {
    buffer_.splice(buffer_.begin(), buffer_, iter->second);
    iter->second->second = current_timestamp_;
    return;
  }
  iter = history_map_.find(frame_id);
  if (iter != history_map_.end()) {
    iter->second->second++;
    if (iter->second->second >= k_) {
      iter->second->second++;
      buffer_.splice(buffer_.begin(), buffer_, iter->second);
      buffer_map_.insert(std::pair(iter->first, iter->second));
      history_map_.erase(iter->first);
    }
    return;
  }
  // while (history_.size() + buffer_.size() >= replacer_size_) {
  //   if (!history_.empty()) {
  //     for (auto iter = history_.rbegin(); iter != history_.rend(); iter++) {
  //       if (non_evictale_[iter->first]) {
  //         non_evictale_.erase(iter->first);
  //         history_map_.erase(iter->first);
  //         history_.remove(*iter);
  //         curr_size_--;
  //       }
  //     }
  //   } else {
  //     for (auto iter = buffer_.rbegin(); iter != buffer_.rend(); iter++) {
  //       if (non_evictale_[iter->first]) {
  //         non_evictale_.erase((iter->first));
  //         buffer_map_.erase(iter->first);
  //         buffer_.remove(*iter);
  //         curr_size_--;
  //       }
  //     }
  //   }
  // }
  history_.push_front(std::pair(frame_id, 1));
  history_map_.insert(std::pair(frame_id, history_.begin()));
  non_evictale_.insert(std::pair(frame_id, true));
  curr_size_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  assert(non_evictale_.find(frame_id) != non_evictale_.end());
  if (set_evictable && !non_evictale_[frame_id]) {
    curr_size_++;
  } else if (!set_evictable && non_evictale_[frame_id]) {
    curr_size_--;
  }
  non_evictale_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::logic_error(std::string("Invalid framed_id in Remove") + std::to_string(frame_id));
  }
  if (non_evictale_.find(frame_id) == non_evictale_.end()) {
    return;
  }
  if (buffer_map_.find(frame_id) != buffer_map_.end()) {
    auto iter = buffer_map_.find(frame_id);
    non_evictale_.erase(iter->first);
    buffer_.erase(iter->second);
    buffer_map_.erase(iter);
    curr_size_--;
    return;
  }
  if (history_map_.find(frame_id) != history_map_.end()) {
    auto iter = history_map_.find(frame_id);
    non_evictale_.erase(iter->first);
    history_.erase(iter->second);
    history_map_.erase(iter);
    curr_size_--;
    return;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
