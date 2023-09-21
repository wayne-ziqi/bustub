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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard guard, int key_index) {
  bpm_ = bpm;
  if (key_index == -1) {
    leaf_page_ = nullptr;
    key_index_ = -1;
    return;
  }
  leaf_page_ = guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  key_index_ = key_index;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_ == nullptr || static_cast<bool>(key_index_ == leaf_page_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(leaf_page_ && key_index_ < leaf_page_->GetSize());
  return leaf_page_->GetItem(key_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  assert(leaf_page_ && key_index_ <= leaf_page_->GetSize());
  if (key_index_ < leaf_page_->GetSize()) {
    ++key_index_;
  } else {
    // if the current leaf page is full, then we need to get the next leaf page
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // if the next page id is invalid, then we have reached the end of the index
      key_index_ = leaf_page_->GetSize();
    } else {
      // otherwise, we need to get the next page
      auto guard = bpm_->FetchPageRead(next_page_id);
      leaf_page_ = guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      key_index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
