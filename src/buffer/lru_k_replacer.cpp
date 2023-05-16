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

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    auto iter = buffer_map_.find(frame_id);
    if(iter != buffer_map_.end()){
        buffer_.splice(buffer_.begin(), buffer_, iter->second);
        return;
    }
    iter = history_map_.find(frame_id);
    if(iter != history_map_.end()){
        iter->second->second++;
        if(iter->second->second >= k_){
            if(buffer_map_.size() == curr_size_){
                frame_id_t del_frame = 
            }
            buffer_map_.insert(std::pair(iter->first, iter->second));
            history_map_.erase(iter->first);
        }
        return;
    }
    
    
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t {
    return replacer_size_;
 }

}  // namespace bustub
