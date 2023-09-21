//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple = Tuple();
  RID child_rid;
  int32_t cnt = 0;
  bool tuple_found = false;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuple_found = true;
    cnt++;
    auto *old_meta = new TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    // first delete the old tuple and then insert the new tuple
    table_info_->table_->UpdateTupleMeta(*old_meta, child_rid);
    auto index_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index : index_vec) {
      index->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid,
          exec_ctx_->GetTransaction());
    }
  }

  *tuple = Tuple(std::vector<Value>{Value(INTEGER, cnt)},
                 new Schema(std::vector<Column>{Column("# of Deleted", TypeId::INTEGER)}));

  return tuple_found;
}

}  // namespace bustub
