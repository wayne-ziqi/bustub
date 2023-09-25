#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (auto *limit_plan = dynamic_cast<const LimitPlanNode *>(optimized_plan.get()); limit_plan != nullptr) {
    if (auto *sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan->GetChildAt(0).get()); sort_plan != nullptr) {
      auto top_n_plan =
          std::make_unique<TopNPlanNode>(std::make_shared<Schema>(sort_plan->OutputSchema()), sort_plan->GetChildPlan(),
                                         sort_plan->GetOrderBy(), limit_plan->GetLimit());
      return top_n_plan;
    }
  }
  return optimized_plan;
}

}  // namespace bustub
