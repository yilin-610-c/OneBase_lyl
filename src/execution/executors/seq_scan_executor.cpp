#include "onebase/execution/executors/seq_scan_executor.h"

#include <stdexcept>
#include <vector>

/*
SELECT * FROM table;
SELECT * FROM table WHERE id > 3;
*/

/*
called by ExecutionEngine
*/

/*
SQL-> SeqScanPlanNode
    -> SeqScanExecutor (by ExecutorFactory)
        -> TableInfo
            -> TableIterator
                -> Tuple
                    -> Predicate (if exists)
                        -> Value (boolean)
                            -> return tuple if true, else continue to next tuple
*/

/*
SeqScanPlanNode：扫描任务的说明书，包含要扫描哪个表、是否有过滤条件等信息
TableInfo：要扫描的表的信息，包括表的模式、表的数据等
TableIterator：表的迭代器，用于遍历表中的每一行数据
Tuple：表中的一行数据，包含多个列的值
Predicate：过滤条件，用于判断当前行数据是否满足条件
Value：过滤条件的结果，通常是一个布尔值，表示当前行数据是否

ExecutorContext:Catalog *catalog_;
BufferPoolManager *bpm_;
Transaction *txn_;
*/

/*
Catalog：
  有哪些 table？
  每个 table 的 schema 是什么？
  每个 table 的 TableHeap 在哪里？
  有哪些 index？
TableInfo
 ├── schema_  ：这张表有哪些列
 ├── name_    ：表名
 ├── table_   ：真正存 tuple 的 TableHeap
 └── oid_     ：表的编号
*/

/*
TableHeap
 ├── Begin()  ：返回一个迭代器，指向表的第一行数据
 ├── End()    ：返回一个迭代器，指向表的最后一行数据的下一个位置
 └── InsertTuple() ：插入一行数据
    GetTuple(...)
    UpdateTuple(...)
    DeleteTuple(...)
*/
namespace onebase {

/*
创建一个 SeqScanExecutor 的时候，需要传进来：
1. ExecutorContext *exec_ctx (工具箱，里面有 Catalog、BufferPoolManager、Transaction 等等)
2. SeqScanPlanNode *plan （扫描计划，里面有要扫描哪个表、是否有过滤条件等信息）
*/
/*
SeqScanExecutor 自己不直接存 exec_ctx_
父类 AbstractExecutor 帮它存
以后通过 GetExecutorContext() 拿回来

自己保存 SeqScanPlanNode 指针，后面扫描时要看这个 plan。
*/
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  //拿到表的编号->从AbstractExecutor拿到执行上下文->从执行上下文拿到目录->从目录拿到表的信息
  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found for sequential scan");
  }

  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &schema = table_info_->schema_;

  while (iter_ != end_) {
    auto current_rid = iter_.GetRID();//拿到当前 tuple 在表里的物理位置
    //RID: page_id + slot_num (这个数据在哪个page的哪个slot)
    auto current_tuple = *iter_;
    ++iter_;

    const auto &predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      //在 current_tuple 这行数据上计算 WHERE 条件
      auto predicate_value = predicate->Evaluate(&current_tuple, &schema);
      if (predicate_value.IsNull() || !predicate_value.GetAsBoolean()) {
        continue;
      }
    }

    // 把 current_tuple 的每一列取出来
    // 重新构造一个新的 Tuple
    // 赋值给输出参数 *tuple
    std::vector<Value> values;
    values.reserve(schema.GetColumnCount());
    for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      values.push_back(current_tuple.GetValue(&schema, i));
    }

    *tuple = Tuple(std::move(values));
    tuple->SetRID(current_rid);
    if (rid != nullptr) {
      *rid = current_rid;
    }
    return true;
  }

  return false;
}

}  // namespace onebase
