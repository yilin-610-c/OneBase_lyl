#include "onebase/execution/executors/sort_executor.h"
#include <algorithm>
#include "onebase/common/exception.h"

/*
/sort_executor.h

plan_:
  这次排序的“说明书”
  里面记录 ORDER BY 哪些表达式、升序还是降序、输出 schema 是什么

child_executor_:
  SortExecutor 下面的 executor
  Sort 本身不直接读表，它从 child_executor_ 一行一行拿输入

sorted_tuples_:
  保存 child executor 输出的所有 tuple
  排序后，这个 vector 就是最终排序结果

cursor_:
  当前已经输出到 sorted_tuples_ 的第几行
*/
/*e.g.
SELECT id, val
FROM base
WHERE id >= 3
ORDER BY val DESC
LIMIT 4;

ExecutionEngine
  -> ProjectionExecutor
      -> LimitExecutor
          -> SortExecutor
              -> SeqScanExecutor

Projection 要一行
  → 问 Limit 要一行
      → Limit 问 Sort 要一行
          → Sort 已经提前把 SeqScan 的所有结果排序好了
*/

/*
exec_ctx:
  executor 的上下文工具箱，比如 catalog、buffer pool、transaction

plan:
  SortPlanNode，告诉 SortExecutor 按什么排序

child_executor:
  SortExecutor 的下层 executor，负责提供待排序的数据
*/

namespace onebase {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


/*
SortPlanNode
  using OrderByType = std::pair<bool, AbstractExpressionRef>;
  std::vector<OrderByType> order_bys_;
GetOrderBys() 返回一个 vector，里面每个元素是一个 pair
  pair.first 是 bool，表示这个表达式是升序还是降序
  pair.second 是 AbstractExpressionRef，表示这个表达式是什么（比如 val、id+1、等等）
*/

void SortExecutor::Init() {
  // TODO(student): Materialize all tuples from child, then sort
  // - Scan all child tuples into sorted_tuples_
  // - Sort using order_by expressions and directions
  // - Reset cursor_ to 0
  child_executor_->Init();//先让下层 executor 准备好
  sorted_tuples_.clear();//不要保留上一次执行的排序结果
  cursor_ = 0;//下一次输出时从第 0 行开始

  //也就是把 child executor 输出的所有 tuple 全部读出来，放进 sorted_tuples_
  Tuple child_tuple;//child executor 输出的 tuple 内容
  RID child_rid;//这行 tuple 的 RID
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    child_tuple.SetRID(child_rid);
    sorted_tuples_.push_back(child_tuple);
  }

  /*
  ORDER BY id ASC, val DESC:
  order_bys = [
  {true,  expression(id)},
  {false, expression(val)}
]*/
  const auto &child_schema = child_executor_->GetOutputSchema();
  const auto &order_bys = plan_->GetOrderBys();
  /*[&child_schema, &order_bys](const Tuple &left, const Tuple &right) {
  ...
  }是一个比较函数（匿名），不断被std::sort调用，
  可以理解为bool CompareTuple(const Tuple &left, const Tuple &right)
  其中[&child_schema, &order_bys]是捕获列表，告诉这个匿名函数要用到外部的 child_schema 和 order_bys 这两个变量
  comparator的参数是两个被比较的tuple
  */
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
            [&child_schema, &order_bys](const Tuple &left, const Tuple &right) {
              for (const auto &[is_ascending, expr] : order_bys) {
                auto left_value = expr->Evaluate(&left, &child_schema);
                auto right_value = expr->Evaluate(&right, &child_schema);

                if (left_value.CompareEquals(right_value).GetAsBoolean()) {
                  continue;
                }

                if (is_ascending) {
                  return left_value.CompareLessThan(right_value).GetAsBoolean();
                }
                return left_value.CompareGreaterThan(right_value).GetAsBoolean();
              }
              return false;
            });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next sorted tuple
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }

  *tuple = sorted_tuples_[cursor_++];
  if (rid != nullptr) {
    *rid = tuple->GetRID();
  }
  return true;
}

}  // namespace onebase
