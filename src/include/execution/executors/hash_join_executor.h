//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleHashJoinHashTable {
 public:
  auto Insert(const HashJoinKey &key, const Tuple &tuple) -> void {
    if (hash_table_.find(key) == hash_table_.end()) {
      hash_table_.insert({key, std::vector<Tuple>{tuple}});
    } else {
      hash_table_[key].emplace_back(tuple);
    }
  }

  class SimpleHashJoinHashTableIterator {
   public:
    explicit SimpleHashJoinHashTableIterator(std::unordered_map<HashJoinKey, std::vector<Tuple>>::iterator iterator)
        : iterator_(iterator) {}

    auto Key() const -> const HashJoinKey & { return iterator_->first; }
    auto Val() const -> const Tuple & { return iterator_->second[index_]; }

    auto operator++() -> SimpleHashJoinHashTableIterator & {
      if (index_ < static_cast<int>(iterator_->second.size()) - 1) {
        index_++;
      } else {
        iterator_++;
        index_ = 0;
      }
      return *this;
    }
    auto operator==(const SimpleHashJoinHashTableIterator &other) const -> bool {
      return iterator_ == other.iterator_ && index_ == other.index_;
    }

   private:
    std::unordered_map<HashJoinKey, std::vector<Tuple>>::iterator iterator_;
    int index_ = 0;
  };

  auto Begin() -> SimpleHashJoinHashTableIterator { return SimpleHashJoinHashTableIterator(hash_table_.begin()); }
  auto End() -> SimpleHashJoinHashTableIterator { return SimpleHashJoinHashTableIterator(hash_table_.end()); }

 private:
  std::unordered_map<HashJoinKey, std::vector<Tuple>> hash_table_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The child executor that produces tuples for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_child_;
  /** The child executor that produces tuples for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_child_;

  /** The hash table used to process the join. */
  SimpleHashJoinHashTable hash_table_;
  /** The iterator pointing to the current position in the hash table. */
  SimpleHashJoinHashTable::SimpleHashJoinHashTableIterator hash_table_iterator_;

  // whether a valid tuple has been emitted, used by left join to avoid emitting duplicate null tuple,
  // inner join will ignore this flag

  bool emitted_valid_tuple_ = false;
};

}  // namespace bustub
