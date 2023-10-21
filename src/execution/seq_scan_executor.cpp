//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      table_iterator_(table_heap_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                         plan_->GetTableOid());
  table_iterator_ = table_heap_->MakeEagerIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iterator_.IsEnd()) {
    // if the current operation is to delete, upgrade the lock to exclusive lock
    if (exec_ctx_->IsDelete()) {
      BUSTUB_ENSURE(
          exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                 plan_->GetTableOid()) &&
              exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                   plan_->GetTableOid(), table_iterator_.GetRID()),
          "Fail to upgrade lock to exclusive for delete")
      //get a shared lock according to isolation level
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
               exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      BUSTUB_ENSURE(exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                         plan_->GetTableOid(), table_iterator_.GetRID()),
                    "Fail to get a shared lock")
    }
    // get the tuple and rid
    *tuple = table_iterator_.GetTuple().second;
    *rid = table_iterator_.GetRID();
    ++table_iterator_;
    // if the tuple is not deleted, release the lock if isolation level is read committed and return true
    if (!table_heap_->GetTupleMeta(*rid).is_deleted_) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), *rid);
      }
      return true;
    } else {
      // if the tuple is deleted, force to release the lock and continue
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), *rid, true);
    }
  }
  return false;
}

}  // namespace bustub
