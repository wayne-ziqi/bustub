//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  /// the table has no lock yet
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    auto fresh_que = std::make_shared<LockRequestQueue>();
    table_lock_map_.emplace(oid, fresh_que);
  }
  auto que = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  que->Lock();
  // check if current txn already has a lock on this table
  /// if so, it's an upgrading request
  if (auto req = que->GetRequest(txn->GetTransactionId()); req != nullptr) {
    if (req->lock_mode_ == lock_mode) {
      que->Unlock();
      return true;
    }
    if (que->upgrading_ != INVALID_TXN_ID) {
      que->Unlock();
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // should drop the old lock and claim the new lock with lock_mode_
    que->upgrading_ = txn->GetTransactionId();
    // the request is already granted, we need to upgrade both the request and the txn
    que->Unlock();
    GrantNewLocksIfPossible(que);
    UpgradeLockTable(txn, req->lock_mode_, lock_mode, oid);
    // FIXME: add latch?
    req->lock_mode_ = lock_mode;
    return true;
  }
  /// the txn doesn't have a lock on this table, it's a new request, add the request into the queue
  auto req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  que->PushBack(req);
  if (que->Size() == 1) {
    que->Unlock();
    GrantNewLocksIfPossible(que);
  } else {
    que->Unlock();
  }
  // IMPORTANT: make sure que latch, txn latch, have been released before wait
  std::unique_lock<std::mutex> lk(que->latch_);
  req->granted_ = CanTxnTakeLock(txn, lock_mode) && que->NewLockCompatible(lock_mode, req->txn_id_);
  if (!req->granted_) {
    que->cv_.wait(lk, [&] {
      return txn->GetState() == TransactionState::ABORTED || (req->granted_ = req->granted_ && CanTxnTakeLock(txn, lock_mode));
    });
  }
  lk.unlock();
  // now we are waken up, we need to check if the txn is aborted
  // officially grant the lock
  txn->LockTxn();
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->UnlockTxn();
    que->Lock();
    que->RemoveRequest(txn->GetTransactionId());
    que->Unlock();
    GrantNewLocksIfPossible(que);
    return false;
  }
  auto lock_set = GetTxnTableLockSet(txn, lock_mode);
  BUSTUB_ASSERT(lock_set->find(oid) == lock_set->end(), "txn should not have a lock on this table");
  lock_set->emplace(oid);
  txn->UnlockTxn();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // check if the txn already has a lock on one of the row
  row_lock_map_latch_.lock();
  txn->LockTxn();
  for (auto &row_lock_pair : row_lock_map_) {
    auto rid = row_lock_pair.first;
    if (txn->IsRowSharedLocked(oid, rid) || txn->IsRowExclusiveLocked(oid, rid)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      row_lock_map_latch_.unlock();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
  }
  txn->UnlockTxn();
  row_lock_map_latch_.unlock();
  // check if a txn has a lock on this table
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto que = table_lock_map_[oid];
  // safely unlock the table from the txn
  que->Lock();
  if (auto req = que->GetRequest(txn->GetTransactionId()); req != nullptr && req->granted_) {
    // remove the lock from the request table
    que->RemoveRequest(txn->GetTransactionId());
    que->Unlock();
    if (table_lock_map_[oid]->Size() == 0) {
      table_lock_map_.erase(oid);
    }
    // FIXME: should we unlock the txn before or after waking up the next request?
    table_lock_map_latch_.unlock();
    // remove the lock from the txn
    txn->LockTxn();
    UpgradeTransactionState(txn, req->lock_mode_);
    auto lock_set = GetTxnTableLockSet(txn, req->lock_mode_);
    BUSTUB_ASSERT(lock_set->find(oid) != lock_set->end(), "txn should have a lock on this table");
    lock_set->erase(oid);
    txn->UnlockTxn();
    // wake up the next request in the queue
    GrantNewLocksIfPossible(que);
    return true;
  } else {
    // the txn doesn't have a lock on this table
    que->Unlock();
    table_lock_map_latch_.unlock();
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  // should never reach here
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  txn->LockTxn();
  // check if the txn is not growing and
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  txn->UnlockTxn();
  CheckAppropriateLockOnTable(txn, oid, lock_mode);
  // check if the table has no lock yet
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    auto fresh_que = std::make_shared<LockRequestQueue>();
    row_lock_map_.emplace(rid, fresh_que);
  }
  auto que = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  que->Lock();
  // check if current txn already has a lock on this row
  /// if so, it's an upgrading request
  if (auto req = que->GetRequest(txn->GetTransactionId()); req != nullptr) {
    if (req->lock_mode_ == lock_mode) {
      que->Unlock();
      return true;
    }
    if (que->upgrading_ != INVALID_TXN_ID) {
      que->Unlock();
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    que->upgrading_ = txn->GetTransactionId();
    // the request is already granted, we need to upgrade both the request and the txn
    que->Unlock();
    GrantNewLocksIfPossible(que);
    UpgradeLockRow(txn, req->lock_mode_, oid, rid);
    // FIXME: add latch?
    req->lock_mode_ = lock_mode;
    return true;
  }

  // the txn doesn't have a lock on this row, it's a new request, add the request into the queue
  auto req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  que->PushBack(req);
  if (que->Size() == 1) {
    que->Unlock();
    GrantNewLocksIfPossible(que);
  } else {
    que->Unlock();
  }
  // IMPORTANT: make sure que latch, txn latch, have been released before wait
  std::unique_lock<std::mutex> lk(que->latch_);
  req->granted_ = CanTxnTakeLock(txn, lock_mode) && que->NewLockCompatible(lock_mode, req->txn_id_);
  if (!req->granted_) {
    que->cv_.wait(lk, [&] {
      return txn->GetState() == TransactionState::ABORTED || (req->granted_ = req->granted_ && CanTxnTakeLock(txn, lock_mode));
    });
  }
  lk.unlock();

  // now we are waken up, we need to check if the txn is aborted
  txn->LockTxn();
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->UnlockTxn();
    que->Lock();
    que->RemoveRequest(txn->GetTransactionId());
    que->Unlock();
    GrantNewLocksIfPossible(que);
    return false;
  }
  // officially grant the lock
  auto lock_set = GetTxnRowLockSet(txn, lock_mode);
  BUSTUB_ASSERT((*lock_set)[oid].find(rid) == (*lock_set)[oid].end(), "txn should not have a lock on this row");
  (*lock_set)[oid].emplace(rid);
  txn->UnlockTxn();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // check if the txn already has a lock on this row
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto que = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  que->Lock();
  if (auto req = que->GetRequest(txn->GetTransactionId()); req != nullptr && req->granted_) {
    // remove the lock from the request table
    que->RemoveRequest(txn->GetTransactionId());
    que->Unlock();
    if (row_lock_map_[rid]->Size() == 0) {
      row_lock_map_.erase(rid);
    }
    // remove the lock from the txn
    txn->LockTxn();
    if (!force) {
      UpgradeTransactionState(txn, req->lock_mode_);
    }
    auto lock_set = GetTxnRowLockSet(txn, req->lock_mode_);
    BUSTUB_ASSERT(lock_set->find(oid) != lock_set->end(), "txn should have a lock on this table");
    BUSTUB_ASSERT((*lock_set)[oid].find(rid) != (*lock_set)[oid].end(), "txn should have a lock on this row");
    (*lock_set)[oid].erase(rid);
    txn->UnlockTxn();
    // wake up the next request in the queue
    GrantNewLocksIfPossible(que);
    return true;
  } else {
    // the txn doesn't have a lock on this row
    que->Unlock();
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  // should never reach here
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::unordered_set<txn_id_t>{t2});
  } else {
    waits_for_[t1].emplace(t2);
  }
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) != waits_for_.end()) {
    waits_for_[t1].erase(t2);
  }
  waits_for_latch_.unlock();
}

auto LockManager::HasCycle(txn_id_t *txn_id_out) -> bool {
  // use dfs to detect cycle
  // use a set to store visited nodes
  // always start from the smallest txn_id
  // if we find a cycle, return true and set txn_id_out to the biggest txn_id in the cycle

  auto cycle = std::unordered_set<txn_id_t>{};
  auto color = std::unordered_map<txn_id_t, int>{};
  auto parents = std::unordered_map<txn_id_t, int>{};
  auto smallest_txn_id = std::numeric_limits<txn_id_t>::max();
  for (const auto &id_pair : waits_for_) {
    if (id_pair.first < smallest_txn_id) {
      smallest_txn_id = id_pair.first;
    }
  }
  bool found = false;
  DFSCycle(smallest_txn_id, INVALID_TXN_ID, color, parents, cycle, found);
  if (found) {
    // return the biggest txn_id in the cycle
    *txn_id_out = *std::max_element(cycle.begin(), cycle.end());
    return true;
  }
  return false;
}

auto LockManager::DFSCycle(txn_id_t cur_txn, txn_id_t par_txn, std::unordered_map<txn_id_t, int> &color,
                           std::unordered_map<txn_id_t, int> &parents, std::unordered_set<txn_id_t> &cycle, bool &found)
    -> void {
  if (color[cur_txn] == 2 || found) {
    return;
  }
  if (color[cur_txn] == 1) {
    txn_id_t u = par_txn;
    cycle.emplace(u);
    while (u != cur_txn) {
      u = parents[u];
      cycle.emplace(u);
    }
    cycle.emplace(cur_txn);
    found = true;
    return;
  }
  color[cur_txn] = 1;
  parents[cur_txn] = par_txn;
  for (auto dst_txn : waits_for_[cur_txn]) {
    DFSCycle(dst_txn, cur_txn, color, parents, cycle, found);
    if (found) {
      return;
    }
  }
  color[cur_txn] = 2;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  waits_for_latch_.lock();
  for (const auto &id_pair : waits_for_) {
    for (auto dst_txn : id_pair.second) {
      edges.emplace_back(id_pair.first, dst_txn);
    }
  }
  waits_for_latch_.unlock();
  return edges;
}

auto LockManager::BuildWaitsFor() -> void {
  // search in row lock map, as the locks are granted in fifo order, if txn0 precedes txn1 in the queue, then add an
  // edge from txn1 to txn0
  waits_for_.clear();
  row_lock_map_latch_.lock();
  for (const auto &row_lock_pair : row_lock_map_) {
    auto que = row_lock_pair.second;
    que->Lock();
    for (auto req0 = que->Begin(); req0 != que->End(); req0++) {
      if (txn_manager_->GetTransaction((*req0)->txn_id_)->GetState() == TransactionState::ABORTED) {
        continue;
      }
      for (auto req1 = req0; req1 != que->End(); req1++) {
        if (req1 == req0 || txn_manager_->GetTransaction((*req1)->txn_id_)->GetState() == TransactionState::ABORTED) {
          continue;
        }
        if ((*req0)->txn_id_ != (*req1)->txn_id_) {
          AddEdge((*req1)->txn_id_, (*req0)->txn_id_);
        }
      }
    }
    que->Unlock();
    que->cv_.notify_all();  // wake up all ABORTED txns
  }
  row_lock_map_latch_.unlock();

  // do the same for table lock map
  table_lock_map_latch_.lock();
  for (const auto &table_lock_pair : table_lock_map_) {
    auto que = table_lock_pair.second;
    que->Lock();
    for (auto req0 = que->Begin(); req0 != que->End(); req0++) {
      if (!(*req0)->granted_ ||
          txn_manager_->GetTransaction((*req0)->txn_id_)->GetState() == TransactionState::ABORTED) {
        continue;
      }
      for (auto req1 = req0; req1 != que->End(); req1++) {
        if (req1 == req0 || !(*req1)->granted_ ||
            txn_manager_->GetTransaction((*req1)->txn_id_)->GetState() == TransactionState::ABORTED) {
          continue;
        }
        if ((*req0)->txn_id_ != (*req1)->txn_id_) {
          AddEdge((*req1)->txn_id_, (*req0)->txn_id_);
        }
      }
    }
    que->Unlock();
    que->cv_.notify_all();  // wake up all ABORTED txns
  }
  table_lock_map_latch_.unlock();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
       // construct waits-for graph
      txn_id_t txn_id_abort;
      BuildWaitsFor();
      while (HasCycle(&txn_id_abort)) {
        auto txn = txn_manager_->GetTransaction(txn_id_abort);
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        BuildWaitsFor();
      }
    }
  }
}

auto LockManager::UpgradeTransactionState(bustub::Transaction *txn, bustub::LockManager::LockMode lock_mode) -> void {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    return;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else {
    BUSTUB_ASSERT(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED, "txn should be read uncommitted");
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
    BUSTUB_ENSURE(lock_mode != LockMode::SHARED,
                  "The behaviour upon unlocking an S lock under READ_UNCOMMITTED is undefined.");
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockManager::LockMode old_mode, LockManager::LockMode lock_mode,
                                   const table_oid_t &oid) -> bool {
  txn->LockTxn();
  if (!CanLockUpgrade(old_mode, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }
  auto old_lock_set = GetTxnTableLockSet(txn, old_mode);
  BUSTUB_ASSERT(old_lock_set->find(oid) != old_lock_set->end(), "txn should have a lock on this table");
  old_lock_set->erase(oid);
  auto new_lock_set = GetTxnTableLockSet(txn, lock_mode);
  new_lock_set->emplace(oid);
  txn->UnlockTxn();
  return true;
}

auto LockManager::UpgradeLockRow(bustub::Transaction *txn, bustub::LockManager::LockMode lock_mode,
                                 const bustub::table_oid_t &oid, const bustub::RID &rid) -> bool {
  txn->LockTxn();
  auto old_lock_mode = GetTxnRowLockMode(txn, oid, rid);
  if (!CanLockUpgrade(old_lock_mode, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }
  auto old_lock_set = GetTxnRowLockSet(txn, old_lock_mode);
  BUSTUB_ASSERT(old_lock_set->find(oid) != old_lock_set->end(), "txn should have a lock on this table");
  (*old_lock_set)[oid].erase(rid);
  auto new_lock_set = GetTxnRowLockSet(txn, lock_mode);
  new_lock_set->emplace(oid, std::unordered_set<RID>{rid});
  txn->UnlockTxn();
  return true;
}

auto LockManager::GetTxnTableLockMode(Transaction *txn, const table_oid_t &oid) -> LockManager::LockMode {
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  BUSTUB_ENSURE(false, "<GetTxnTableLockMode> should not reach here");
}

auto LockManager::GetTxnTableLockSet(Transaction *txn, LockManager::LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->GetExclusiveTableLockSet();
  }
  if (lock_mode == LockMode::SHARED) {
    return txn->GetSharedTableLockSet();
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return txn->GetIntentionExclusiveTableLockSet();
  }
  if (lock_mode == LockMode::INTENTION_SHARED) {
    return txn->GetIntentionSharedTableLockSet();
  }
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return txn->GetSharedIntentionExclusiveTableLockSet();
  }
  BUSTUB_ENSURE(false, "<GetTxnTableLockSet> should not reach here");
}

auto LockManager::GetTxnRowLockMode(bustub::Transaction *txn, const bustub::table_oid_t &oid, const bustub::RID &rid)
    -> LockMode {
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  BUSTUB_ENSURE(false, "<GetTxnRowLockMode> should not reach here");
}

auto LockManager::GetTxnRowLockSet(bustub::Transaction *txn, bustub::LockManager::LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->GetExclusiveRowLockSet();
  }
  if (lock_mode == LockMode::SHARED) {
    return txn->GetSharedRowLockSet();
  }
  BUSTUB_ENSURE(false, "<GetTxnRowLockSet> should not reach here");
}

auto LockManager::AreLocksCompatible(bustub::LockManager::LockMode l1, bustub::LockManager::LockMode l2) -> bool {
  if (l1 == LockMode::INTENTION_SHARED) {
    return l2 != LockMode::EXCLUSIVE;
  }
  if (l1 == LockMode::INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
  }
  if (l1 == LockMode::SHARED) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
  }
  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED;
  }
  BUSTUB_ENSURE(l1 == LockMode::EXCLUSIVE, "l1 should be exclusive");
  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      return false;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED)) {
      return false;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
      return false;
    }
    if (txn->GetState() != TransactionState::GROWING) {
      return false;
    }
  }
  return true;
}

void LockManager::GrantNewLocksIfPossible(std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  // first try to grant the update request
  std::lock_guard<std::mutex> lk(lock_request_queue->latch_);
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    auto upgrade_req = lock_request_queue->GetRequest(lock_request_queue->upgrading_);
    auto lock_mode = upgrade_req->lock_mode_;
    upgrade_req->granted_ = lock_request_queue->NewLockCompatible(lock_mode, upgrade_req->txn_id_);
    if (upgrade_req->granted_) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      // inform the waiting txn
      lock_request_queue->cv_.notify_all();
    }
    return;
  }
  // requests at the front of the queue should all be granted,
  // check all requests behind the front ones and grant them if possible
  // stop when we meet a request whose lock mode is not compatible with the granted requests'
  for (auto iter = lock_request_queue->GetFirstUnGrantedIter(); iter != lock_request_queue->End(); iter++) {
    auto req = *iter;
    BUSTUB_ASSERT(req->granted_ == false, "the request should not be granted yet");
    auto lock_mode = req->lock_mode_;
    req->granted_ = lock_request_queue->NewLockCompatible(lock_mode, req->txn_id_);
    if (req->granted_) {
      // inform the waiting txn
      lock_request_queue->cv_.notify_all();
    } else {
      break;
    }
  }
}
auto LockManager::CanLockUpgrade(LockManager::LockMode curr_lock_mode, LockManager::LockMode requested_lock_mode)
    -> bool {
  /**
   * While upgrading, only the following transitions should be allowed:
   * IS -> [S, X, IX, SIX]
   * S -> [X, SIX]
   * IX -> [X, SIX]
   * SIX -> [X]
   * */
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
           requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (curr_lock_mode == LockMode::SHARED) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE;
  }

  return false;
}
auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid,
                                              LockManager::LockMode row_lock_mode) -> bool {
  auto txn_throw = [txn](txn_id_t txn_id, AbortReason reason) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, reason);
  };

  // the txn should have a lock on the table
  if (row_lock_mode == LockMode::INTENTION_SHARED || row_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      row_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn_throw(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  auto table_lock_mode = GetTxnTableLockMode(txn, oid);
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    if (table_lock_mode != LockMode::EXCLUSIVE && table_lock_mode != LockMode::INTENTION_EXCLUSIVE &&
        table_lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn_throw(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  if (row_lock_mode == LockMode::SHARED) {
    if (table_lock_mode == LockMode::EXCLUSIVE) {
      txn_throw(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  return true;
}

}  // namespace bustub
