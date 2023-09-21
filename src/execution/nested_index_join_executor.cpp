//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    if (!child_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }

    // get the index of inner table
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
    auto inner_tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
    std::vector<RID> inner_rids;
    inner_tree->ScanKey(left_tuple_.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                 index_info->index_->GetKeyAttrs()),
                        &inner_rids, exec_ctx_->GetTransaction());
    for (auto inner_rid : inner_rids) {
      auto inner_tuple_pack = inner_table_info->table_->GetTuple(inner_rid);
      if (!inner_tuple_pack.first.is_deleted_) {
        right_tuple = inner_tuple_pack.second;
        right_rid = inner_rid;
        auto pred_val = plan_->KeyPredicate()->EvaluateJoin(&left_tuple_, child_executor_->GetOutputSchema(),
                                                            &right_tuple, inner_table_info->schema_);
        if (!pred_val.IsNull() && pred_val.GetAs<bool>()) {
          std::vector<Value> values;
          values.reserve(GetOutputSchema().GetColumnCount());
          for (int i = 0; i < static_cast<int>(child_executor_->GetOutputSchema().GetColumnCount()); ++i) {
            values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
          }
          for (int i = 0; i < static_cast<int>(inner_table_info->schema_.GetColumnCount()); ++i) {
            values.push_back(right_tuple.GetValue(&inner_table_info->schema_, i));
          }
          *tuple = Tuple(values, &GetOutputSchema());
          return true;
        }
      }
    }
  }
  // never reach here
  return false;
}

}  // namespace bustub
