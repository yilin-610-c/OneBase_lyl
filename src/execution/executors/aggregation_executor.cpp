#include "onebase/execution/executors/aggregation_executor.h"
#include "onebase/common/exception.h"

#include <string>
#include <utility>
#include <vector>

/*plan_:
  AggregationPlanNode，告诉 executor：
  - GROUP BY 表达式有哪些
  - aggregate 表达式有哪些
  - aggregate 类型是什么，比如 COUNT / SUM / MIN / MAX

child_executor_:
  aggregation 的输入来源。
  比如 SELECT COUNT(*) FROM base WHERE id >= 3，
  child 可能是 SeqScanExecutor，负责先筛出 id >= 3 的 tuple。

result_tuples_:
  保存最终聚合结果。
  如果没有 GROUP BY，通常只有一行。
  如果有 GROUP BY，通常每个 group 一行。

cursor_:
  Next() 已经输出到 result_tuples_ 的第几行。*/

namespace onebase {

namespace {

  // AggregationState 保存每个 group 的中间聚合结果
struct AggregationState {
  std::vector<Value> group_values_;
  std::vector<Value> aggregate_values_;
};

// 把 group by 列的值拼成一个字符串，作为 hash table 的 key
auto MakeGroupKey(const std::vector<Value> &group_values) -> std::string {
  std::string key;
  for (const auto &value : group_values) {
    key += value.ToString();
    key += "#";
  }
  return key;
}

// 根据聚合类型和输入值类型，返回这个聚合的初始值
auto MakeInitialAggregateValue(AggregationType agg_type, TypeId value_type) -> Value {
  switch (agg_type) {
    case AggregationType::CountStarAggregate:
    case AggregationType::CountAggregate:
      return Value(TypeId::INTEGER, 0);
    case AggregationType::SumAggregate:
    case AggregationType::MinAggregate:
    case AggregationType::MaxAggregate:
      return Value(value_type);
  }
  return Value();
}

// COUNT(*) 和 COUNT(expr) 的初始值都是 0，每遇到一行满足条件的输入，计数就加 1
//因为这里的 count 是数据库内部的 Value 对象，不是普通 int
auto IncrementCount(const Value &count) -> Value {
  return Value(TypeId::INTEGER, count.GetAsInteger() + 1);
}

}  // namespace

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                          std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  // TODO(student): Initialize child and build aggregation hash table
  // - Scan all tuples from child
  // - Group by group_by expressions
  // - Compute aggregates (COUNT, SUM, MIN, MAX) per group
  child_executor_->Init();
  result_tuples_.clear();
  cursor_ = 0;

  const auto &child_schema = child_executor_->GetOutputSchema();
  const auto &group_bys = plan_->GetGroupBys();//GROUP BY 表达式列表
  const auto &aggregates = plan_->GetAggregates();//aggregate 参数表达式列表
  const auto &agg_types = plan_->GetAggregateTypes();//与aggregate 参数一一对应的聚合类型列表，比如 COUNT / SUM / MIN / MAX

  /*ELECT id, COUNT(*), SUM(val)
    FROM base
    GROUP BY id;

    扫描过程中可能有：
    groups["1#"] = {
      group_values_ = [1],
      aggregate_values_ = [2, 21]
    }
    groups["2#"] = {
      group_values_ = [2],
      aggregate_values_ = [1, 20]
    }

    也就是：
    id = 1 的 group 当前 count=2, sum=21
    id = 2 的 group 当前 count=1, sum=20*/
  std::unordered_map<std::string, AggregationState> groups;

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> group_values;
    //计算当前 tuple 属于哪个 group
    group_values.reserve(group_bys.size());
    for (const auto &expr : group_bys) {
      group_values.push_back(expr->Evaluate(&child_tuple, &child_schema));
    }

    //用 group values 生成 group key
    /*group_values = [1] 得到：group_key = "1#"
    如果：group_values = [] 得到：group_key = ""*/
    auto group_key = MakeGroupKey(group_values);

    /*group_iter:
      指向这个 group 的 iterator。
      如果是新插入的，它指向新 group。
      如果原来已经存在，它指向原来的 group。

    inserted:
      bool。
      true 表示这次真的插入了新 group。
      false 表示这个 group 之前已经存在。*/
    auto [group_iter, inserted] = groups.emplace(group_key, AggregationState{});
    auto &state = group_iter->second;

    //如果是新 group，要初始化它的 state
    if (inserted) {
      //保存这个 group 的 group by 值
      state.group_values_ = std::move(group_values);
      /*如果这个SQL是：
        SELECT COUNT(*), SUM(val), MIN(val), MAX(val)
        FROM base
        GROUP BY id;
        则每个 group 需要维护四个 aggregate 当前值
        aggregate_values_[0] = COUNT(*)
        aggregate_values_[1] = SUM(val)
        aggregate_values_[2] = MIN(val)
        aggregate_values_[3] = MAX(val)*/
      state.aggregate_values_.reserve(agg_types.size());
      for (size_t i = 0; i < agg_types.size(); ++i) {
        state.aggregate_values_.push_back(
            MakeInitialAggregateValue(agg_types[i], aggregates[i]->GetReturnType()));
      }
    }

    for (size_t i = 0; i < agg_types.size(); ++i) {
      auto &current = state.aggregate_values_[i];
      switch (agg_types[i]) {
        case AggregationType::CountStarAggregate:
          current = IncrementCount(current);
          break;
        case AggregationType::CountAggregate: {
          auto value = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!value.IsNull()) {
            current = IncrementCount(current);
          }
          break;
        }
        case AggregationType::SumAggregate: {
          auto value = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!value.IsNull()) {
            current = current.IsNull() ? value : current.Add(value);
          }
          break;
        }
        case AggregationType::MinAggregate: {
          auto value = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!value.IsNull() &&
              (current.IsNull() || value.CompareLessThan(current).GetAsBoolean())) {
            current = value;
          }
          break;
        }
        case AggregationType::MaxAggregate: {
          auto value = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!value.IsNull() &&
              (current.IsNull() || value.CompareGreaterThan(current).GetAsBoolean())) {
            current = value;
          }
          break;
        }
      }
    }
  }

  if (groups.empty() && group_bys.empty()) {
    std::vector<Value> values;
    values.reserve(agg_types.size());
    for (size_t i = 0; i < agg_types.size(); ++i) {
      values.push_back(MakeInitialAggregateValue(agg_types[i], aggregates[i]->GetReturnType()));
    }
    result_tuples_.emplace_back(std::move(values));
    return;
  }

  for (auto &[group_key, state] : groups) {
    std::vector<Value> values;
    values.reserve(state.group_values_.size() + state.aggregate_values_.size());
    for (const auto &value : state.group_values_) {
      values.push_back(value);
    }
    for (const auto &value : state.aggregate_values_) {
      values.push_back(value);
    }
    result_tuples_.emplace_back(std::move(values));
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next aggregation result
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
