#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), top_entries_() {
  std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, ReverseTupleComparator> pq(
      ReverseTupleComparator(plan_->GetOrderBy(), plan_->OutputSchema()));
  top_entries_.reserve(plan_->GetN());
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq.emplace(tuple, rid);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    top_entries_.push_back(pq.top());
    pq.pop();
  }
  std::reverse(top_entries_.begin(), top_entries_.end());
  iter_ = top_entries_.begin();
}

void TopNExecutor::Init() {
  child_executor_->Init();
  iter_ = top_entries_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == top_entries_.end()) {
    return false;
  }
  *tuple = iter_->first;
  *rid = iter_->second;
  ++iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); }

}  // namespace bustub
