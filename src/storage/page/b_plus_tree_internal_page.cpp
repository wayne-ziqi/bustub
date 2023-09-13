//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  array_ = std::vector<MappingType>(max_size + 1);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index != 0);
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index != 0);
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize() + 1; ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator)
    -> int {
  int key_index = 1;
  assert(GetSize() < GetMaxSize());
  while (key_index <= GetSize() && comparator(array_[key_index].first, key) < 0) {
    key_index++;
  }
  for (int i = GetSize() + 1; i > key_index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[key_index] = {key, value};
  IncreaseSize(1);
  return key_index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetLeftMostPageId(page_id_t page_id) -> void { array_[0].second = page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *recipient) -> KeyType {
  assert(GetSize() == GetMaxSize());
  int mid = GetSize() / 2 + 1;
  KeyType key = KeyAt(mid);
  for (int i = mid + 1; i < GetSize() + 1; ++i) {
    recipient->array_[i - mid] = array_[i];
  }
  recipient->SetLeftMostPageId(ValueAt(mid));
  recipient->SetSize(GetSize() - mid);
  SetSize(mid - 1);
  return key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  // use binary search
  int left = 1;
  int right = GetSize();
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      return mid;
    } else if (comparator(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) -> bool {
  if (index <= 0 || index > GetSize()) {
    return false;
  }
  for (int i = index; i < GetSize(); ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveRightAllToLeft(BPlusTreeInternalPage *left, KeyType key) -> void {
  // move all elements to left
  int index = left->GetSize();
  left->array_[index + 1] = {key, ValueAt(0)};
  for (int i = 1; i <= GetSize(); ++i) {
    left->array_[index + i + 1] = array_[i];
  }
  left->IncreaseSize(GetSize());
  SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
