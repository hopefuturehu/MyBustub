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
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <string>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (history_.empty() && buffer_.empty()) {
    return false;
  }
  if (history_.empty()) {
    auto del_frame = std::prev(buffer_.end());
    *frame_id = del_frame->first;
    buffer_map_.erase(del_frame->first);
    buffer_.pop_back();
  } else {
    auto del_frame = std::prev(history_.end());
    *frame_id = del_frame->first;
    history_map_.erase(del_frame->first);
    history_.pop_back();
  }
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::logic_error(std::string("Invalid framed_id") + std::to_string(frame_id));
  }
  current_timestamp_++;
  if (non_evictale_.find(frame_id) != non_evictale_.end()) {
    auto iter = non_evictale_.find(frame_id);
    iter->second = current_timestamp_;
    return;
  }
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
      iter->second->second = current_timestamp_;
      while (buffer_map_.size() + curr_size_ >= replacer_size_) {
        frame_id_t del_frame = std::prev(buffer_.end())->first;
        buffer_.pop_back();
        buffer_map_.erase(del_frame);
      }
      buffer_.splice(buffer_.begin(), buffer_, iter->second);
      buffer_map_.insert(std::pair(iter->first, iter->second));
      history_map_.erase(iter->first);
    }
    return;
  }
  while (history_.size() + buffer_.size() >= replacer_size_) {
    if (history_.empty()) {
      auto del_frame = std::prev(buffer_.end());
      buffer_map_.erase(del_frame->first);
      buffer_.erase(del_frame);
    } else {
      auto del_frame = std::prev(history_.end());
      history_map_.erase(del_frame->first);
      history_.erase(del_frame);
    }
  }
  history_.push_front(std::pair(frame_id, 1));
  history_map_.insert(std::pair(frame_id, history_.begin()));
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (set_evictable) {
    curr_size_++;
  } else {
    if (non_evictale_.size() > replacer_size_) {
      throw std::logic_error(std::string("too many non_evictable page"));
    }
  }
  auto ne_iter = non_evictale_.find(frame_id);
  if (ne_iter != non_evictale_.end()) {
    if (!set_evictable) {
      return;
    }  // put page back to buffer_list]
    for (auto ind = buffer_.begin(); ind != buffer_.end(); ind++) {
      if (ind->second < ne_iter->second) {
        buffer_.insert(ind, std::pair(frame_id, ne_iter->second));
        ind--;
        buffer_map_.insert(std::pair(frame_id, ind));
        non_evictale_.erase(frame_id);
        return;
      }
    }
    buffer_.emplace_back(std::pair(ne_iter->first, ne_iter->second));
    buffer_map_.insert(std::pair(ne_iter->first, std::prev(buffer_.end())));
    non_evictale_.erase(frame_id);
    return;
  }
  auto buff_iter = buffer_map_.find(frame_id);
  if (buff_iter != buffer_map_.end()) {
    if (!set_evictable) {
      non_evictale_.insert(std::pair(buff_iter->first, buff_iter->second->second));
      buffer_.erase(buff_iter->second);
      buffer_map_.erase(buff_iter);
      curr_size_--;
    }
    return;
  }
  auto his_iter = history_map_.find(frame_id);
  if (his_iter != history_map_.end()) {
    if (!set_evictable) {
      non_evictale_.insert(std::pair(his_iter->first, his_iter->second->second));
      history_.erase(his_iter->second);
      history_map_.erase(his_iter);
      curr_size_--;
    }
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (non_evictale_.find(frame_id) != non_evictale_.end()) {
    throw std::logic_error(std::string("Can't remove en invictable frame") + std::to_string(frame_id));
    return;
  }
  if (buffer_map_.find(frame_id) != buffer_map_.end()) {
    auto iter = buffer_map_.find(frame_id);
    buffer_.erase(iter->second);
    buffer_map_.erase(iter);
    return;
  }
  if (history_map_.find(frame_id) != history_map_.end()) {
    auto iter = history_map_.find(frame_id);
    history_.erase(iter->second);
    history_map_.erase(iter);
    return;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return buffer_.size() + history_.size();
}

}  // namespace bustub
