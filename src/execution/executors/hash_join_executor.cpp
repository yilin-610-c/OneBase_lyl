#include "onebase/execution/executors/hash_join_executor.h"
#include "onebase/common/exception.h"

#include <vector>

/*先把 left tuples 按 join key 分组，放进 hash table
然后扫描 right tuples：
  用 right key 去 hash table 里直接查匹配的 left tuples*/

/*HashJoinExecutor
包含std::unordered_map<std::string, std::vector<Tuple>> hash_table_;
表示：
join key -> 所有具有这个 key 的左边 tuple*/
/*HashJoinPlanNode*/

namespace onebase {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                    std::unique_ptr<AbstractExecutor> left_executor,
                                    std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void HashJoinExecutor::Init() {
  // TODO(student): Build hash table from left child, initialize right child
  hash_table_.clear();
  result_tuples_.clear();
  cursor_ = 0;

  left_executor_->Init();
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    auto key = plan_->GetLeftKeyExpression()->Evaluate(&left_tuple, &left_schema).ToString();
    hash_table_[key].push_back(left_tuple);
  }

  right_executor_->Init();
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    auto key = plan_->GetRightKeyExpression()->Evaluate(&right_tuple, &right_schema).ToString();
    auto matches = hash_table_.find(key);
    if (matches == hash_table_.end()) {
      continue;
    }

    for (const auto &matched_left_tuple : matches->second) {
      std::vector<Value> values;
      values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(matched_left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }

      result_tuples_.emplace_back(std::move(values));
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Probe hash table with right child tuples
  // - Phase 1 (in Init): Build hash table from left child on left_key_expr
  // - Phase 2 (in Next): For each right tuple, probe hash table using right_key_expr
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }

  *tuple = result_tuples_[cursor_++];
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase
