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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    return false;
  }
  latch_.lock();
  size_t max_distance = 0;
  size_t max_inf_distance = 0;
  frame_id_t max_frame_id = 0;
  for (auto &node : node_store_) {
    if (node.second.IsEvictable()) {
      auto backward_k_distance = node.second.GetBackwardKDistance(curr_timestamp_);
      if (backward_k_distance < std::numeric_limits<size_t>::max()) {
        // If the node has been accessed for k times, we use the backward k distance to break ties.
        if (backward_k_distance > max_distance) {
          max_distance = backward_k_distance;
          max_frame_id = node.first;
        }
      } else {
        // If the node has not been accessed for k times, we use the least recent distance to break ties.
        max_distance = backward_k_distance;
        auto least_recent_distance = node.second.GetLeastRecentDistance(curr_timestamp_);
        if (least_recent_distance > max_inf_distance) {
          max_inf_distance = least_recent_distance;
          max_frame_id = node.first;
        }
      }
    }
  }
  *frame_id = max_frame_id;
  node_store_.erase(max_frame_id);
  --curr_size_;
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(frame_id, k_);
  }
  node_store_[frame_id].AddHistory(curr_timestamp_);
  ++curr_timestamp_;
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    return;
  }
  auto node = node_store_[frame_id];
  if (node.IsEvictable() == set_evictable) {
    latch_.unlock();
    return;
  }
  if (set_evictable) {
    ++curr_size_;
  } else {
    --curr_size_;
  }
  node_store_[frame_id].SetEvictable(set_evictable);
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    return;
  }
  auto node = node_store_[frame_id];
  if (node.IsEvictable()) {
    node_store_.erase(frame_id);
    --curr_size_;
    latch_.unlock();
  } else {
    latch_.unlock();
    throw Exception(ExceptionType::EXECUTION, "Cannot remove a non-evictable node");
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
