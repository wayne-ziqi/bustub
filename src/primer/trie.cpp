#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> node = root_;
  if (node == nullptr) {
    return nullptr;
  }
  for (char c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      return nullptr;
    }
    node = node->children_.at(c);
  }
  if (node->children_.find('\0') == node->children_.end()) {
    return nullptr;
  }

  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node->children_.at('\0').get());
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  // clone the root
  std::shared_ptr<const TrieNode> newroot = root_ == nullptr ? std::make_unique<TrieNode>() : Clone(root_);
  std::shared_ptr<const TrieNode> node = newroot;

  for (char c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      auto new_node = std::make_shared<TrieNode>();
      auto node_mod = std::const_pointer_cast<TrieNode>(node);
      node_mod->children_.insert(std::make_pair(c, new_node));
      node = new_node;
    } else {
      node = node->children_.at(c);
    }
  }
  auto value_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  auto node_mod = std::const_pointer_cast<TrieNode>(node);
  if (node->children_.find('\0') != node->children_.end()) {
    node_mod->children_.erase('\0');
  }
  node_mod->children_.insert(std::make_pair('\0', value_node));
  return Trie(newroot);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<const TrieNode> newroot = root_ == nullptr ? std::make_unique<TrieNode>() : Clone(root_);
  std::shared_ptr<const TrieNode> node = newroot;
  std::vector<std::pair<std::shared_ptr<const TrieNode>, char>> path;
  for (char c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      return Trie(newroot);
    }
    path.emplace_back(node, c);
    node = node->children_.at(c);
  }
  if (node->children_.find('\0') == node->children_.end()) {
    return Trie(newroot);
  }
  path.emplace_back(node, '\0');
  auto node_mod = std::const_pointer_cast<TrieNode>(node);
  node_mod->children_.erase('\0');
  for (auto it = path.rbegin(); it != path.rend(); ++it) {
    if (it->first->children_.empty()) {
      auto parent_mod = std::const_pointer_cast<TrieNode>(it->first);
      parent_mod->children_.erase(it->second);
    } else if (it->first->children_.size() == 1 && it->first->children_.find('\0') != it->first->children_.end()) {
      auto parent_mod = std::const_pointer_cast<TrieNode>(it->first);
      auto child = it->first->children_.at('\0');
      parent_mod->children_.erase('\0');
      parent_mod->children_.insert(std::make_pair(it->second, child));
    } else {
      break;
    }
  }
  return Trie(newroot);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
