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
  table_iterator_ = table_heap_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iterator_.IsEnd()) {
    *tuple = table_iterator_.GetTuple().second;
    *rid = table_iterator_.GetRID();
    ++table_iterator_;
    if (!table_heap_->GetTupleMeta(*rid).is_deleted_) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
