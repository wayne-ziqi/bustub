#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return GetRootPageId() == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageRead(const KeyType &key, Transaction *txn) -> ReadPageGuard {
  page_id_t root_page_id = GetRootPageId();
  // Fetch the root page.
  auto guard = bpm_->FetchPageRead(root_page_id);
  // Search the  B+ from the root page
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    // Find the child page.
    for (int i = 1; i <= internal_page->GetSize() + 1; ++i) {
      if (i == internal_page->GetSize() + 1 || comparator_(internal_page->KeyAt(i), key) > 0) {
        // Fetch the child page.
        guard = bpm_->FetchPageRead(internal_page->ValueAt(i - 1));
        page = guard.template As<BPlusTreePage>();
        break;
      }
    }
  }
  return guard;
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;

  if (IsEmpty()) {
    return false;
  }
  auto guard = GetLeafPageRead(key, txn);
  auto page = guard.template As<BPlusTreePage>();

  // Now the page is a leaf page.
  auto leaf_page = reinterpret_cast<const LeafPage *>(page);
  assert(leaf_page->IsLeafPage());
  int idx = leaf_page->KeyIndex(key, comparator_);
  if (idx != -1) {
    result->push_back(leaf_page->ValueAt(idx));
    return true;
  }
  // If the key is not found, return false.
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageWrite(const KeyType &key, bool is_insert, std::vector<WritePageGuard> &concur_vec,
                                      std::vector<page_id_t> *path, Transaction *txn) -> WritePageGuard {
  auto root_page_id = GetRootPageId();
  auto guard = bpm_->FetchPageWrite(root_page_id);
  auto cur_page = guard.template AsMut<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    concur_vec.push_back(std::move(guard));
    if (path != nullptr) {
      path->push_back(concur_vec.back().PageId());
    }
    auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
    if ((is_insert && internal_page->GetSize() < internal_page->GetMaxSize()) ||
        (!is_insert && internal_page->GetSize() > internal_page->GetMinSize())) {
      // current node is safe, so we should release write latch
      auto tmp_guard = std::move(concur_vec.back());
      while (!concur_vec.empty()) {
        concur_vec.pop_back();
      }
      concur_vec.push_back(std::move(tmp_guard));
    }
    for (int i = 1; i <= internal_page->GetSize() + 1; ++i) {
      if (i == internal_page->GetSize() + 1 || comparator_(internal_page->KeyAt(i), key) > 0) {
        // Fetch the child page.
        guard = bpm_->FetchPageWrite(internal_page->ValueAt(i - 1));
        cur_page = guard.template AsMut<BPlusTreePage>();
        break;
      }
    }
  }
  return guard;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetWriteGuardFromConVec(std::vector<WritePageGuard> &concur_vec, page_id_t pid) -> WritePageGuard {
  // find the guard in the concur_vec, remove it from the concur_vec and return it.
  for (int i = 0; i < static_cast<int>(concur_vec.size()); ++i) {
    if (concur_vec[i].PageId() == pid) {
      auto guard = std::move(concur_vec[i]);
      concur_vec.erase(concur_vec.begin() + i);
      return guard;
    }
  }
  return bpm_->FetchPageWrite(pid);
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  if (IsEmpty()) {
    // If the tree is empty, create a new leaf page.
    auto pg_guard = bpm_->NewPageGuarded(&ctx.root_page_id_);
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = ctx.root_page_id_;
    auto leaf_page = pg_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->Insert(key, value, comparator_);
    return true;
  }
  std::vector<page_id_t> path;
  std::vector<WritePageGuard> concur_vec;
  auto leaf_guard = GetLeafPageWrite(key, true, concur_vec, &path, txn);
  auto *leaf_page = leaf_guard.template AsMut<LeafPage>();
  assert(leaf_page->IsLeafPage());
  int idx = leaf_page->KeyIndex(key, comparator_);
  if (idx != -1) {
    return false;
  }
  // Insert the key, value pair into the leaf page.
  leaf_page->Insert(key, value, comparator_);
  // if the leaf page is not full, return true.
  if (leaf_page->GetSize() != leaf_page->GetMaxSize()) {
    return true;
  }
  // leaf page is full, split the leaf page and adjust the parent page recursively.
  // split the leaf page into two leaf pages, and insert the middle key into the parent page.
  // if the parent page is full, split the parent page and adjust the grandparent page recursively.
  // if the parent page is the root page, create a new root page.
  page_id_t right_page_id;
  page_id_t left_page_id = leaf_guard.PageId();
  auto right_guard = bpm_->NewPageGuarded(&right_page_id);
  auto right_page = right_guard.AsMut<LeafPage>();
  right_page->Init(leaf_max_size_);
  // move the right half of the keys and values to the new page.
  leaf_page->Split(right_page);
  right_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(right_page_id);
  KeyType middle_key = right_page->KeyAt(0);
  // insert the middle key into the parent page and point to the
  do {
    if (path.empty()) {
      page_id_t root_page_id;
      auto header_guard = bpm_->FetchPageWrite(header_page_id_);
      auto header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
      auto root_guard = bpm_->NewPageGuarded(&root_page_id);
      auto root_page = root_guard.template AsMut<InternalPage>();
      root_page->Init(internal_max_size_);
      root_page->SetLeftMostPageId(left_page_id);
      root_page->Insert(middle_key, right_page_id, comparator_);
      header_page->root_page_id_ = root_page_id;
      return true;
    }
    auto parent_guard = GetWriteGuardFromConVec(concur_vec, path.back());
    path.pop_back();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    parent_page->Insert(middle_key, right_page_id, comparator_);
    if (parent_page->GetMaxSize() != parent_page->GetSize()) {
      return true;
    }
    // split the parent page.
    right_guard = bpm_->NewPageGuarded(&right_page_id);
    auto right_inter_page = right_guard.template AsMut<InternalPage>();
    right_inter_page->Init(internal_max_size_);
    middle_key = parent_page->Split(right_inter_page);
    left_page_id = parent_guard.PageId();
  } while (true);

  // unreachable
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    return;
  }
  // Get the root page id.
  //  auto root_page_id = GetRootPageId();
  //  ctx.root_page_id_ = root_page_id;
  //  auto guard = bpm_->FetchPageBasic(root_page_id);
  //  auto cur_page = guard.template As<BPlusTreePage>();
  std::vector<page_id_t> path;
  std::vector<WritePageGuard> concur_vec;
  auto leaf_guard = GetLeafPageWrite(key, false, concur_vec, &path, txn);
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();
  assert(leaf_page->IsLeafPage());
  int idx = leaf_page->KeyIndex(key, comparator_);
  if (idx == -1) {
    return;
  }
  leaf_page->Remove(idx);
  if (leaf_guard.PageId() == GetRootPageId()) {
    // check if the root page is empty.
    if (leaf_page->GetSize() == 0) {
      auto header_guard = bpm_->FetchPageWrite(header_page_id_);
      auto header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = INVALID_PAGE_ID;
      leaf_guard.Drop();
      bpm_->DeletePage(leaf_guard.PageId());
    }
    return;
  }
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    // search the parent page and replace the key with the first key in the leaf page.
    while (!path.empty()) {
      auto parent_guard = GetWriteGuardFromConVec(concur_vec, path.back());
      path.pop_back();
      auto parent_page = parent_guard.template AsMut<InternalPage>();
      idx = parent_page->KeyIndex(key, comparator_);
      if (idx != -1) {
        parent_page->SetKeyAt(idx, leaf_page->KeyAt(0));
        break;
      }
    }
    return;
  }

  // try to borrow from left/right sibling
  auto parent_guard = GetWriteGuardFromConVec(concur_vec, path.back());
  auto parent_page = parent_guard.template AsMut<InternalPage>();
  idx = parent_page->ValueIndex(leaf_guard.PageId());
  assert(idx != -1);
  if (idx > 0) {
    auto left_guard = bpm_->FetchPageWrite(parent_page->ValueAt(idx - 1));
    auto left_page = left_guard.template AsMut<LeafPage>();
    assert(left_page->IsLeafPage());
    if (left_page->GetSize() > left_page->GetMinSize()) {
      // borrow the last key and value from the left page
      auto last_key = left_page->KeyAt(left_page->GetSize());
      auto last_value = left_page->ValueAt(left_page->GetSize());
      left_page->Remove(left_page->GetSize());
      leaf_page->Insert(last_key, last_value, comparator_);
      parent_page->SetKeyAt(idx, last_key);
      return;
    }
  }
  if (idx < parent_page->GetSize()) {
    auto right_guard = bpm_->FetchPageWrite(parent_page->ValueAt(idx + 1));
    auto right_page = right_guard.template AsMut<LeafPage>();
    assert(right_page->IsLeafPage());
    if (right_page->GetSize() > right_page->GetMinSize()) {
      // borrow the first key and value from the right page and change the key in the parent page.
      auto first_key = right_page->KeyAt(0);
      auto first_value = right_page->ValueAt(0);
      right_page->Remove(0);
      leaf_page->Insert(first_key, first_value, comparator_);
      parent_page->SetKeyAt(idx, leaf_page->KeyAt(0));  // in case the deleted key is the first key in the leaf page.
      parent_page->SetKeyAt(idx + 1, right_page->KeyAt(0));
      return;
    }
  }

  // merge the leaf page with the left or right page.

  if (idx > 0) {
    auto left_guard = bpm_->FetchPageWrite(parent_page->ValueAt(idx - 1));
    auto left_page = left_guard.template AsMut<LeafPage>();
    assert(left_page->IsLeafPage());
    leaf_page->MoveRightAllToLeft(left_page);
    left_page->SetNextPageId(leaf_page->GetNextPageId());
    parent_page->Remove(idx);
    leaf_guard.Drop();
    bpm_->DeletePage(leaf_guard.PageId());
    leaf_guard = std::move(left_guard);
  } else {
    auto right_guard = bpm_->FetchPageWrite(parent_page->ValueAt(idx + 1));
    auto right_page = right_guard.template AsMut<LeafPage>();
    assert(right_page->IsLeafPage());
    right_page->MoveRightAllToLeft(leaf_page);
    leaf_page->SetNextPageId(right_page->GetNextPageId());
    parent_page->Remove(idx + 1);
    right_guard.Drop();
    bpm_->DeletePage(right_guard.PageId());
  }
  // search if some parent page has key, change it to the first key in the leaf page, note:
  // the key must be the minimum key in the subtree, it must occur in the left most leaf page.
  idx = parent_page->KeyIndex(key, comparator_);
  if (idx != -1) {
    parent_page->SetKeyAt(idx, leaf_page->KeyAt(0));
  } else {
    for (int i = 1; i < static_cast<int>(path.size()); ++i) {
      parent_guard = GetWriteGuardFromConVec(concur_vec, path[i]);
      parent_page = parent_guard.template AsMut<InternalPage>();
      idx = parent_page->KeyIndex(key, comparator_);
      if (idx != -1) {
        parent_page->SetKeyAt(idx, leaf_page->KeyAt(0));
        break;
      }
    }
  }

  // fix the parent page recursively, the merged page is now leaf page.
  parent_guard = GetWriteGuardFromConVec(concur_vec, path.back());
  path.pop_back();
  while (path.size() > 1) {
    parent_page = parent_guard.template AsMut<InternalPage>();
    if (parent_page->GetSize() >= parent_page->GetMinSize()) {
      // the parent page does not underflow, return.
      return;
    }
    auto grand_guard = GetWriteGuardFromConVec(concur_vec, path.back());
    auto grand_page = grand_guard.template AsMut<InternalPage>();
    path.pop_back();
    idx = grand_page->ValueIndex(parent_guard.PageId());
    assert(idx != -1);
    if (idx > 0) {
      // try to borrow from the left sibling.
      auto left_guard = bpm_->FetchPageBasic(grand_page->ValueAt(idx - 1));
      auto left_page = left_guard.template AsMut<InternalPage>();
      if (left_page->GetSize() > left_page->GetMinSize()) {
        // steal the key of the grand page, the last key of the left page rise to the grand page, the value of the last
        // key of the left page becomes the leftmost value of the parent page.
        auto last_key = left_page->KeyAt(left_page->GetSize());
        auto last_value = left_page->ValueAt(left_page->GetSize());
        left_page->Remove(left_page->GetSize());
        parent_page->Insert(grand_page->KeyAt(idx), parent_page->ValueAt(0), comparator_);
        parent_page->SetLeftMostPageId(last_value);
        grand_page->SetKeyAt(idx, last_key);
        return;
      }
    }
    if (idx < grand_page->GetSize()) {
      // try to borrow from the right sibling.
      auto right_guard = bpm_->FetchPageBasic(grand_page->ValueAt(idx + 1));
      auto right_page = right_guard.template AsMut<InternalPage>();
      if (right_page->GetSize() > right_page->GetMinSize()) {
        // steal the key of the grand page, the first key of the right page rise to the grand page, the value of the
        // first key of the right page becomes the rightmost value of the parent page.
        auto first_key = right_page->KeyAt(1);
        auto first_value = right_page->ValueAt(0);
        right_page->SetLeftMostPageId(right_page->ValueAt(1));
        right_page->Remove(1);
        parent_page->Insert(grand_page->KeyAt(idx + 1), first_value, comparator_);
        grand_page->SetKeyAt(idx + 1, first_key);
        return;
      }
    }
    // merge the parent page with the left or right page.
    if (idx > 0) {
      auto left_guard = bpm_->FetchPageBasic(grand_page->ValueAt(idx - 1));
      auto left_page = left_guard.template AsMut<InternalPage>();
      parent_page->MoveRightAllToLeft(left_page, grand_page->KeyAt(idx));
      grand_page->Remove(idx);
      parent_guard.Drop();
      bpm_->DeletePage(parent_guard.PageId());
    } else {
      auto right_guard = bpm_->FetchPageBasic(grand_page->ValueAt(idx + 1));
      auto right_page = right_guard.template AsMut<InternalPage>();
      right_page->MoveRightAllToLeft(parent_page, grand_page->KeyAt(idx + 1));
      grand_page->Remove(idx + 1);
      parent_guard.Drop();
      bpm_->DeletePage(parent_guard.PageId());
    }
    parent_guard = std::move(grand_guard);
  }

  assert(parent_guard.PageId() == GetRootPageId());
  parent_page = parent_guard.template AsMut<InternalPage>();

  // the parent page is the root page.
  if (parent_page->GetSize() == 0) {
    // the root page is empty, delete the root page and set the root page id to invalid.
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = leaf_guard.PageId();
    parent_guard.Drop();
    bpm_->DeletePage(parent_guard.PageId());
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto root_page_id = GetRootPageId();
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageRead(internal_page->ValueAt(0));
    page = guard.template As<BPlusTreePage>();
  }
  // get the first key value pair
  auto leaf_page = reinterpret_cast<const LeafPage *>(page);
  assert(leaf_page->IsLeafPage());
  return INDEXITERATOR_TYPE(bpm_, std::move(guard), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto guard = GetLeafPageRead(key);
  auto page = guard.template As<BPlusTreePage>();
  // get the index of the key
  auto leaf_page = reinterpret_cast<const LeafPage *>(page);
  assert(leaf_page->IsLeafPage());
  int idx = leaf_page->KeyIndex(key, comparator_);
  if (idx == leaf_page->GetSize()) {
    return End();
  }
  return INDEXITERATOR_TYPE(bpm_, std::move(guard), idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto root_page_id = GetRootPageId();
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageRead(internal_page->ValueAt(internal_page->GetSize()));
    page = guard.template As<BPlusTreePage>();
  }
  // get the last key value pair
  auto leaf_page = reinterpret_cast<const LeafPage *>(page);
  assert(leaf_page->IsLeafPage());
  return INDEXITERATOR_TYPE(bpm_, std::move(guard), leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize() + 1; i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
