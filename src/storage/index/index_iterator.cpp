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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index) {
  page_ = page;
  buffer_pool_manager_ = bpm;
  index_ = index;
  if (page != nullptr) {
    leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
  } else {
    leaf_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    // page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(page_->GetPageId() != INVALID_PAGE_ID, "page_id_ != INVALID_PAGE_ID");
  BUSTUB_ASSERT(page_->GetPageId() == leaf_->GetPageId(), "page_id_ == leaf_page_->GetPageId()");  // should match
  // BUSTUB_ASSERT(leaf_->GetSize() > index_, "leaf_page_->GetSize() > idx_");
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_->GetNextPageId() != INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1) {
    assert(leaf_->GetNextPageId() != INVALID_PAGE_ID);
    auto next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());
    // next_page->RLatch();
    // page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = next_page;
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return leaf_ == nullptr || (leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_);
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
