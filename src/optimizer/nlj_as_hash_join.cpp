#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    // check if the join predicate is a conjunction of equality predicates
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    const auto &predicate = nlj_plan.Predicate();
    if (predicate == nullptr) {
      return optimized_plan;
    }

    auto ordered_col_expr = [](const AbstractExpressionRef &left, const AbstractExpressionRef &right)
        -> std::pair<const ColumnValueExpression *, const ColumnValueExpression *> {
      auto *left_expr = dynamic_cast<const ColumnValueExpression *>(left.get());
      auto *right_expr = dynamic_cast<const ColumnValueExpression *>(right.get());
      if (left_expr->GetTupleIdx() > right_expr->GetTupleIdx()) {
        return std::make_pair(right_expr, left_expr);
      }
      return std::make_pair(left_expr, right_expr);
    };

    if (const auto *conjunction = dynamic_cast<const LogicExpression *>(predicate.get()); conjunction != nullptr) {
      if (conjunction->logic_type_ == LogicType::And && conjunction->GetChildren().size() == 2) {
        const auto &left = dynamic_cast<const ComparisonExpression &>(*conjunction->GetChildAt(0));
        const auto &right = dynamic_cast<const ComparisonExpression &>(*conjunction->GetChildAt(1));
        if (left.comp_type_ == ComparisonType::Equal && right.comp_type_ == ComparisonType::Equal) {
          auto [left_left, left_right] = ordered_col_expr(left.GetChildAt(0), left.GetChildAt(1));
          auto [right_left, right_right] = ordered_col_expr(right.GetChildAt(0), right.GetChildAt(1));
          // fix the order of the join key expressions according to tuple id

          // create a hash join plan
          // construct left join key expressions and right join key expressions
          std::vector<AbstractExpressionRef> left_join_key_exprs;
          std::vector<AbstractExpressionRef> right_join_key_exprs;
          left_join_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, left_left->GetColIdx(), left_left->GetReturnType()));
          left_join_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, left_right->GetColIdx(), left_right->GetReturnType()));
          right_join_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, right_left->GetColIdx(), right_left->GetReturnType()));
          right_join_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, right_right->GetColIdx(), right_right->GetReturnType()));
          auto hash_join_plan = std::make_shared<HashJoinPlanNode>(
              std::make_shared<Schema>(nlj_plan.OutputSchema()), nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
              left_join_key_exprs, right_join_key_exprs, nlj_plan.GetJoinType());
          return hash_join_plan;
        }
      }
    }
    if (const auto *comparison = dynamic_cast<const ComparisonExpression *>(predicate.get()); comparison != nullptr) {
      if (comparison->comp_type_ == ComparisonType::Equal) {
        auto [left, right] = ordered_col_expr(comparison->GetChildAt(0), comparison->GetChildAt(1));
        // create a hash join plan
        // construct left join key expressions and right join key expressions
        std::vector<AbstractExpressionRef> left_join_key_exprs;
        std::vector<AbstractExpressionRef> right_join_key_exprs;
        left_join_key_exprs.emplace_back(
            std::make_shared<ColumnValueExpression>(0, left->GetColIdx(), left->GetReturnType()));
        right_join_key_exprs.emplace_back(
            std::make_shared<ColumnValueExpression>(0, right->GetColIdx(), right->GetReturnType()));
        auto hash_join_plan = std::make_shared<HashJoinPlanNode>(
            std::make_shared<Schema>(nlj_plan.OutputSchema()), nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
            left_join_key_exprs, right_join_key_exprs, nlj_plan.GetJoinType());
        return hash_join_plan;
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
