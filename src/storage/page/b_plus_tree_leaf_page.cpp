//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetMaxSize(max_size);
  this->SetParentPageId(parent_id);
  this->SetPageId(page_id);
  this->SetSize(0);
  this->SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { 
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

/*
 * helper method to get value of key
 * @param V the value you want to get

*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &K, ValueType &V, const KeyComparator &comparator) const -> bool {
  int l = 0;
  int r = GetSize();
  if(l >= r) {
    if(comparator(array_[r].first, K) == 0){
      V = array_[r].second;
      return true;
    }
    return false;
  }
  while(l < r) {
    int mid = (l + r) / 2;
    if(comparator(array_[mid].first, K) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  if(comparator(array_[r].first, K) == 0){
    V = array_[r].second;
    return true;
  }
  return false;
}

// 默认Insert不会插入重复值
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &K, const ValueType &V, const KeyComparator &comparator) const -> bool {
  int l = 0;
  int r = GetSize();
  if(l < r) {
    *(array_ + r) = {K, V};
  } else {
    while(l < r) {
      int mid = (l + r) / 2;
      if(comparator(array_[mid].first, K) < 0) {
        l = mid + 1;
      } else {
        r = mid;
      }
    }
  }

}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
