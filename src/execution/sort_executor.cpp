#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &t1, const Tuple &t2) {
    const auto &order_bys = plan_->GetOrderBy();
    for (const auto &order_by : order_bys) {
      const auto &expr = order_by.second;
      const auto &type = order_by.first;

      const auto &schema = GetOutputSchema();
      const auto &val1 = expr->Evaluate(&t1, schema);
      const auto &val2 = expr->Evaluate(&t2, schema);
      if (val1.CompareEquals(val2) == CmpBool::CmpFalse) {
        if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
          return val1.CompareLessThan(val2) == CmpBool::CmpTrue;
        }
        return val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
      }
    }
    return false;
  });
  iter_ = tuples_.begin();
}

void SortExecutor::Init() { iter_ = tuples_.begin(); }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *iter_;
  ++iter_;
  return true;
}

}  // namespace bustub
