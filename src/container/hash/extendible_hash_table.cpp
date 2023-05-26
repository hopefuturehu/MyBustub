//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int bucket_num = IndexOf(key);
  return dir_[bucket_num]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int ind = IndexOf(key);
  return dir_[ind]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  /*
  1.尝试插入Key，若插入成功，返回即可，若不成功，执行步骤2。
  2.判断当前IndexOf(key)指向的bucket下，该bucket是否满了。如果满了，执行步骤3。否则执行步骤7。
  3.如果当前global depth等于local depth，说明bucket已满，需要增长direcotry的大小。增加directory的global depth，
    并将新增加的entry链接到对应的bucket。否则，继续执行步骤4。
  4.记录当前的local mask，创建bucket1和bucket2，增加两个bucket的local depth，增加num bucket的数量。
    取出之前满了的bucket中的元素，按照local mask的标准将每个元素重新分配到bucket1和bucket2中。执行步骤5。
  5.对每个链接到产生overflow的bucket的direcotry entry，按照local mask的标准，重新分配指针指向。执行步骤6。
  6.重新计算IndexOf(key)，执行步骤2。
  7.插入指定的key/value pair。
  */
  size_t dir_index;
  std::shared_ptr<Bucket> target_bucket;
  V val;
  if (dir_[IndexOf(key)]->Find(key, val)) {
    dir_[IndexOf(key)]->Insert(key, value);
    return;
  }
  while (dir_[IndexOf(key)]->IsFull()) {
    dir_index = IndexOf(key);
    target_bucket = dir_[dir_index];
    if (GetGlobalDepthInternal() == GetLocalDepthInternal(dir_index)) {
      global_depth_++;
      size_t len = dir_.size();
      for (size_t i = 0; i < len; ++i) {
        dir_.emplace_back(dir_[i]);
      }
    }
    size_t local_mask = (1 << (GetLocalDepthInternal(dir_index)));
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    for (const auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & local_mask) != 0) {
        one_bucket->Insert(item.first, item.second);
      } else {
        zero_bucket->Insert(item.first, item.second);
      }
    }

    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == target_bucket) {
        if ((i & local_mask) == 0) {
          dir_[i] = zero_bucket;
        } else {
          dir_[i] = one_bucket;
        }
      }
    }

    num_buckets_++;
  }

  dir_index = IndexOf(key);
  target_bucket = dir_[dir_index];
  bool nill = target_bucket->Insert(key, value);
  assert(nill);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.remove_if(
          [&](std::pair<K, V> iter) -> bool { return iter.first == key; });  // list_.remove(iter) seems alse work
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      iter->second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(std::pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
