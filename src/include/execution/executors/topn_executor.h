//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/* Comparator used by the heap, note that the compare function should be reverse to the true order*/
class ReverseTupleComparator {
 public:
  ReverseTupleComparator(const std::vector<std::pair<OrderByType, std::shared_ptr<AbstractExpression>>> &group_bys,
                  const Schema &schema)
      : group_bys_(group_bys), schema_(schema) {}

  auto operator()(const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) const -> bool {
    for (const auto &group_by : group_bys_) {
      const auto &expr = group_by.second;
      const auto &order_by_type = group_by.first;
      const auto &a_val = expr->Evaluate(&a.first, schema_);
      const auto &b_val = expr->Evaluate(&b.first, schema_);
      if(a_val.CompareEquals(b_val) == CmpBool::CmpFalse) {
        if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
          return a_val.CompareLessThan(b_val) == CmpBool::CmpTrue;
        }
        return a_val.CompareGreaterThan(b_val) == CmpBool::CmpTrue;
      }
    }
    return false;
  }

 private:
  const std::vector<std::pair<OrderByType, std::shared_ptr<AbstractExpression>>> &group_bys_;
  const Schema &schema_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The container to store top entries */
  std::vector<std::pair<Tuple, RID>> top_entries_;

  /** The iterator to the top_entries_ container */
  std::vector<std::pair<Tuple, RID>>::iterator iter_;
};
}  // namespace bustub
