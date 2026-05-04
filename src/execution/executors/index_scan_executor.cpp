#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/common/exception.h"

#include <stdexcept>
#include <vector>

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // TODO(student): Initialize index scan using the B+ tree index
  auto *catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found for index scan");
  }

  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  if (index_info_ == nullptr) {
    throw std::runtime_error("Index not found for index scan");
  }

  matching_rids_.clear();
  cursor_ = 0;

  if (!index_info_->SupportsPointLookup()) {
    return;
  }

  auto lookup_key = plan_->GetLookupKey()->Evaluate(nullptr, nullptr).GetAsInteger();
  const auto *rids = index_info_->LookupInteger(lookup_key);
  if (rids != nullptr) {
    matching_rids_ = *rids;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next tuple from index scan
  const auto &schema = table_info_->schema_;

  while (cursor_ < matching_rids_.size()) {
    auto current_rid = matching_rids_[cursor_++];
    auto raw_tuple = table_info_->table_->GetTuple(current_rid);

    const auto &predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      auto predicate_value = predicate->Evaluate(&raw_tuple, &schema);
      if (predicate_value.IsNull() || !predicate_value.GetAsBoolean()) {
        continue;
      }
    }

    std::vector<Value> values;
    values.reserve(schema.GetColumnCount());
    for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      values.push_back(raw_tuple.GetValue(&schema, i));
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
