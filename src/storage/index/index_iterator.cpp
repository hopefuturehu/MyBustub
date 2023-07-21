/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index) {
  page_id_ = page_id;
  buffer_pool_manager_ = bpm;
  index_ = index;
  if (page_id != INVALID_PAGE_ID) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
  } else {
    leaf_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_id_ != INVALID_PAGE_ID) {
    // page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(page_id_ != INVALID_PAGE_ID, "page_id_ != INVALID_PAGE_ID");
  BUSTUB_ASSERT(page_id_ == leaf_->GetPageId(), "page_id_ == leaf_page_->GetPageId()");  // should match
  // BUSTUB_ASSERT(leaf_->GetSize() > index_, "leaf_page_->GetSize() > idx_");
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_->GetNextPageId() != INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1) {
    auto next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());
    // next_page->RLatch();
    // page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id_, false);
    page_id_ = next_page->GetPageId();
    leaf_ = reinterpret_cast<LeafPage *>(next_page->GetData());
    index_ = 0;
  } else if (leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
    leaf_ = nullptr;
    page_id_ = INVALID_PAGE_ID;
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return leaf_ == nullptr || (page_id_ == itr.page_id_ && index_ == itr.index_);
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
