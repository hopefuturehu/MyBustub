//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = 0;
  if (GetAvailableFrame(&frame_id)) {
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    disk_manager_->ReadPage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].pin_count_ = 1;
    return &pages_[frame_id];
  }
  if (GetAvailableFrame(&frame_id)) {
    page_table_->Insert(page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    disk_manager_->ReadPage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].pin_count_ = 1;
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].pin_count_ <= 0) {
      return false;
    }
    pages_[frame_id].pin_count_--;
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
      pages_[frame_id].is_dirty_ = is_dirty;
    }
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (page_table_->Find(i, frame_id)) {
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
        pages_[frame_id].is_dirty_ = false;
      }
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].pin_count_ > 0) {
      return false;
    }

    page_table_->Remove(page_id);
    free_list_.emplace_back(frame_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    *out_frame_id = fid;
    return true;
  }

  if (replacer_->Evict(&fid)) {
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      pages_[fid].is_dirty_ = false;
    }
    page_table_->Remove(pages_[fid].page_id_);
    *out_frame_id = fid;
    pages_[fid].ResetMemory();
    return true;
  }
  return false;
}

}  // namespace bustub
