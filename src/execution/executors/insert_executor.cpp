#include "onebase/execution/executors/insert_executor.h"
#include "onebase/common/exception.h"

#include <stdexcept>
#include <vector>

/*InsertExecutor包含：
plan_:
  Insert 的计划说明书。
  它告诉 InsertExecutor 要插入到哪张表。

child_executor_:
  提供待插入数据的下层 executor。
  比如 INSERT INTO copy SELECT * FROM base WHERE id <= 2，
  child_executor_ 就负责执行 SELECT * FROM base WHERE id <= 2。

has_inserted_:
  标记这次插入是否已经执行过。
  因为 InsertExecutor 只能执行一次插入，不能每次 Next 都重复插入。

InsertPlanNode提供：
  table_oid_:
    要插入到哪张表
    target table = copy
  child:
    插入的数据从哪里来
    child plan = SeqScanPlanNode(base, predicate id <= 2)
*/

namespace onebase {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}
    /*AbstractExecutor(exec_ctx):
    调用父类构造函数，把 exec_ctx 保存到父类里。
    plan_(plan):
      保存 InsertPlanNode 指针。
    child_executor_(std::move(child_executor)):
    把 child executor 的所有权移动进 InsertExecutor。*/

void InsertExecutor::Init() {
  // TODO(student): Initialize child executor
  child_executor_->Init();
  has_inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Insert tuples from child into the table
  // - Get tuples from child, insert into table_heap
  // - Update any indexes
  // - Return count of inserted rows as a single integer tuple
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  /*
  GetExecutorContext() 是从父类 AbstractExecutor 拿执行上下文。
  GetCatalog() 是从上下文里拿 Catalog（包含目标表位置和目标表的索引）*/
  auto *catalog = GetExecutorContext()->GetCatalog();
  /*plan_->GetTableOid() 就是 copy 这张表的 oid
  catalog->GetTable(...) 根据 oid 找到 TableInfo*/
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    throw std::runtime_error("Table not found for insert");
  }

  const auto &schema = table_info->schema_;
  auto indexes = catalog->GetTableIndexes(table_info->name_);//拿到表行所有IndexInfo

  int32_t insert_count = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    //把 child executor 输出的 tuple 插入到目标表里, 得到这个 tuple 在目标表里的 RID
    auto inserted_rid = table_info->table_->InsertTuple(child_tuple);
    if (!inserted_rid.has_value()) {
      throw std::runtime_error("Failed to insert tuple");
    }

    //每插入一行，都要同步更新目标表上的所有相关索引
    //逐个维护这些索引
    for (auto *index_info : indexes) {
      if (!index_info->SupportsPointLookup()) {
        continue;
      }
      auto key_attr = index_info->GetLookupAttr(); //返回这个 index 建在哪一列上
      auto key = child_tuple.GetValue(&schema, key_attr).GetAsInteger();// 从插入的 tuple 里取出索引 key
      index_info->InsertEntry(key, inserted_rid.value());//把这个 key 和新 RID 加进对应 index
    }

    ++insert_count;
  }

  *tuple = Tuple(std::vector<Value>{Value(TypeId::INTEGER, insert_count)});
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase

/*
TableInfo  = 一张表
IndexInfo  = 一张表上的一个索引
key        = 某个索引列上的具体值
RID        = 某一行 tuple 在 TableHeap 里的物理位置

一个表可以有多个IndexInfo
student table
  ├── IndexInfo: idx_student_id     key column = id
  ├── IndexInfo: idx_student_age    key column = age
  └── IndexInfo: idx_student_score  key column = score

IndexInfo包含：
  Schema key_schema_;
  std::string name_; 这个索引叫什么名字，比如 idx_student_id
  std::string table_name_;  这个索引属于哪张表，比如 student
  index_oid_t oid_;
  std::vector<uint32_t> key_attrs_;  这个索引建立在哪些列上 在当前 Lab 3 简化版中，主要支持单列 integer index
  std::unordered_map<int32_t, std::vector<RID>> int_rid_map_; 真正保存 key -> RID list 的结构 （某个整数 key 值 -> 所有拥有这个 key 的 tuple 的 RID）

  Table
  ├── IndexInfo A: index on id
  │       ├── key 1 -> [RID...]
  │       ├── key 2 -> [RID...]
  │       └── key 3 -> [RID...]
  │
  └── IndexInfo B: index on age
          ├── key 18 -> [RID...]
          ├── key 20 -> [RID...]
          └── key 21 -> [RID...]
*/