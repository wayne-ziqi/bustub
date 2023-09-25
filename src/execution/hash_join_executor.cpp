//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      hash_table_iterator_(hash_table_.Begin()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    HashJoinKey key;
    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      key.keys_.emplace_back(expr->Evaluate(&left_tuple, left_child_->GetOutputSchema()).Copy());
    }
    hash_table_.Insert(key, left_tuple);
  }
  emitted_valid_tuple_ = false;
  hash_table_iterator_ = hash_table_.Begin();
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  emitted_valid_tuple_ = false;
  hash_table_iterator_ = hash_table_.Begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // scan over all tuples in the right table
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    if (hash_table_iterator_ == hash_table_.End()) {
      return false;
    }
    while (right_child_->Next(&right_tuple, &right_rid)) {
      HashJoinKey key;
      for (auto &expr : plan_->RightJoinKeyExpressions()) {
        key.keys_.emplace_back(expr->Evaluate(&right_tuple, right_child_->GetOutputSchema()).Copy());
      }
      if (hash_table_iterator_.Key() == key) {
        std::vector<Value> vals;
        vals.reserve(plan_->OutputSchema().GetColumnCount());
        auto &left_child_schema = left_child_->GetOutputSchema();
        auto &right_child_schema = right_child_->GetOutputSchema();
        for (int i = 0; i < static_cast<int>(left_child_schema.GetColumnCount()); ++i) {
          vals.emplace_back(hash_table_iterator_.Val().GetValue(&left_child_schema, i));
        }
        for (int i = 0; i < static_cast<int>(right_child_schema.GetColumnCount()); ++i) {
          vals.emplace_back(right_tuple.GetValue(&right_child_schema, i));
        }
        emitted_valid_tuple_ = true;
        *tuple = Tuple(vals, &plan_->OutputSchema());
        return true;
      }
    }
    right_child_->Init();
    if (plan_->GetJoinType() == JoinType::LEFT && !emitted_valid_tuple_) {
      std::vector<Value> vals;
      vals.reserve(plan_->OutputSchema().GetColumnCount());
      auto &left_child_schema = left_child_->GetOutputSchema();
      auto &right_child_schema = right_child_->GetOutputSchema();
      for (int i = 0; i < static_cast<int>(left_child_schema.GetColumnCount()); i++) {
        vals.emplace_back(hash_table_iterator_.Val().GetValue(&left_child_schema, i));
      }
      for (int i = 0; i < static_cast<int>(right_child_schema.GetColumnCount()); i++) {
        vals.emplace_back(ValueFactory::GetNullValueByType(right_child_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple(vals, &plan_->OutputSchema());
      ++hash_table_iterator_;
      return true;
    }
    emitted_valid_tuple_ = false;
    ++hash_table_iterator_;
  }
  // never reach here
  return false;
}

}  // namespace bustub
