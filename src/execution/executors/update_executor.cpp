#include "onebase/execution/executors/update_executor.h"
#include "onebase/common/exception.h"

#include <stdexcept>
#include <vector>

namespace onebase {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  // TODO(student): Initialize child executor
  child_executor_->Init();
  has_updated_ = false;
}

/*
UPDATE copy SET val = val + 100 WHERE id = 1;
UpdatePlanNode
  target table: copy
  child plan: SeqScan(copy, predicate id = 1)
  update_exprs:
    第 0 列 id  的新值 = old.id
    第 1 列 val 的新值 = old.val + 100
*/

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Update tuples using update expressions
  // - Get tuples from child, evaluate update expressions, update table_heap
  // - Return count of updated rows
  if (has_updated_) {
    return false;
  }
  has_updated_ = true;

  //从catalog里拿到目标表的信息（包括目标表的位置和目标表的索引）
  auto *catalog = GetExecutorContext()->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    throw std::runtime_error("Table not found for update");
  }

  //schema用于：
  // 1. 计算 update expressions 时解释 old_tuple 的列。
  //2. 更新 index 时，从 old_tuple / new_tuple 中取 index key。
  const auto &schema = table_info->schema_;
  auto indexes = catalog->GetTableIndexes(table_info->name_);

  int32_t update_count = 0;
  Tuple old_tuple;
  RID tuple_rid;
  //child executor 每次返回一条“应该被更新的旧 tuple”
  //child 会把下一条匹配 WHERE 的旧 tuple 写进 old_tuple，
  // 把它在目标表里的 RID 写进 tuple_rid
  while (child_executor_->Next(&old_tuple, &tuple_rid)) {
    std::vector<Value> new_values;
    const auto &update_exprs = plan_->GetUpdateExpressions();//得到每一列的新值的表达式
    new_values.reserve(update_exprs.size());
    for (const auto &expr : update_exprs) {
      new_values.push_back(expr->Evaluate(&old_tuple, &schema));
      //expr->Evaluate(&old_tuple, &schema)返回这个表达式在 old_tuple 上的计算结果，也就是这一列的新值
    }
    Tuple new_tuple(std::move(new_values));

    for (auto *index_info : indexes) {
      if (!index_info->SupportsPointLookup()) {
        continue;
      }
      auto key_attr = index_info->GetLookupAttr();
      auto old_key = old_tuple.GetValue(&schema, key_attr).GetAsInteger();
      index_info->RemoveEntry(old_key, tuple_rid);
    }

    //真正修改表里的数据
    if (!table_info->table_->UpdateTuple(tuple_rid, new_tuple)) {
      throw std::runtime_error("Failed to update tuple");
    }

    for (auto *index_info : indexes) {
      if (!index_info->SupportsPointLookup()) {
        continue;
      }
      auto key_attr = index_info->GetLookupAttr();
      auto new_key = new_tuple.GetValue(&schema, key_attr).GetAsInteger();
      index_info->InsertEntry(new_key, tuple_rid);
    }

    ++update_count;
  }

  *tuple = Tuple(std::vector<Value>{Value(TypeId::INTEGER, update_count)});
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase
