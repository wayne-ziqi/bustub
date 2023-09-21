//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      child_executor_(std::move(child_executor)) {}
// As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.

void UpdateExecutor::Init() {}

auto UpdateExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  Tuple child_tuple = Tuple();
  RID child_rid;
  bool tuple_found = false;
  int32_t cnt = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    cnt++;
    auto *old_meta = new TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    // first delete the old tuple and then insert the new tuple
    table_info_->table_->UpdateTupleMeta(*old_meta, child_rid);
    auto *insert_meta = new TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    std::vector<Value> values;
    for (const auto &expr : plan_->target_expressions_) {
      auto val = expr->Evaluate(&child_tuple, table_info_->schema_);
      values.push_back(val);
    }
    auto *new_tuple = new Tuple(values, &table_info_->schema_);
    auto new_rid = table_info_->table_->InsertTuple(*insert_meta, *new_tuple);
    if (new_rid == std::nullopt) {
      return false;
    }
    auto index_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index : index_vec) {
      index->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid,
          exec_ctx_->GetTransaction());
      index->index_->InsertEntry(
          new_tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          new_rid.value(), exec_ctx_->GetTransaction());
    }
  }

  *tuple = Tuple(std::vector<Value>{Value(INTEGER, cnt)},
                 new Schema(std::vector<Column>{Column("# of Updated", TypeId::INTEGER)}));

  return tuple_found;
}

}  // namespace bustub
