//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())) {}

void InsertExecutor::Init() {}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple = Tuple();
  RID child_rid;
  bool tuple_found = false;
  int32_t cnt = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuple_found = true;
    cnt++;
    auto *meta = new TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto insert_rid = table_info_->table_->InsertTuple(*meta, child_tuple, nullptr, nullptr, 0);
    if (insert_rid == std::nullopt) {
      return false;
    }
    auto index_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index : index_vec) {
      index->index_->InsertEntry(
          child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          insert_rid.value(), exec_ctx_->GetTransaction());
    }
  }

  *tuple = Tuple(std::vector<Value>{Value(INTEGER, cnt)},
                 new Schema(std::vector<Column>{Column("# of Inserted", TypeId::INTEGER)}));

  return tuple_found;
}

}  // namespace bustub
