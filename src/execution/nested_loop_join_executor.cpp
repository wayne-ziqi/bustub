//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  int left_col_count = static_cast<int>(left_executor_->GetOutputSchema().GetColumnCount());
  int right_col_count = static_cast<int>(right_executor_->GetOutputSchema().GetColumnCount());
  while (true) {
    if (!may_have_more_) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
    }
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto pred_val = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                       right_executor_->GetOutputSchema());
      if (!pred_val.IsNull() && pred_val.GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (int i = 0; i < left_col_count; ++i) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (int i = 0; i < right_col_count; ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        may_have_more_ = true;
        return true;
      }
    }
    right_executor_->Init();
    if (plan_->GetJoinType() == JoinType::LEFT && !may_have_more_) {
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (int i = 0; i < left_col_count; ++i) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      // leave the columns in the right table to null
      for (int i = 0; i < right_col_count; ++i) {
        values.push_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      may_have_more_ = false;
      return true;
    }
    may_have_more_ = false;
  }
  // never reach here
  return false;
}

}  // namespace bustub
