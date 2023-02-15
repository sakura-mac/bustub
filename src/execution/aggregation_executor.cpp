//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_it_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  // bug fix
  aht_.Clear();
  Tuple child_tuple{};
  RID rid;

  // 1. get the child executor's returning result and stored into hash table( can be used in Next: SELECT t,x, MAX( t.y)
  // FROM t)
  while (child_->Next(&child_tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    aht_.InsertWithoutCombine(MakeAggregateKey(&child_tuple), aht_.GenerateInitialAggregateValue());
  }
  aht_it_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_it_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values{};
  values.reserve(aht_it_.Key().group_bys_.size() + aht_it_.Val().aggregates_.size());
  for (auto &key : aht_it_.Key().group_bys_) {
    values.emplace_back(key);
  }
  for (auto &value : aht_it_.Val().aggregates_) {
    values.emplace_back(value);
  }
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_it_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
