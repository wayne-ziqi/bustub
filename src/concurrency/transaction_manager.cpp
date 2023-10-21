//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"

namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  while (!txn->GetWriteSet()->empty()) {
    auto wr_record = txn->GetWriteSet()->back();
    txn->GetWriteSet()->pop_back();
    if (wr_record.wtype_ == WType::INSERT) {
      wr_record.table_heap_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, wr_record.rid_);
    } else if (wr_record.wtype_ == WType::DELETE) {
      wr_record.table_heap_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, wr_record.rid_);
    } else {
      throw NotImplementedException("update is not implemented!");
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);

}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
