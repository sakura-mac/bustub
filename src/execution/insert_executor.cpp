//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), is_inserted_(false) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  is_inserted_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_inserted_) {
    return false;
  }
  is_inserted_ = true;

  // get the all corresponding indexs to update
  std::string name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
  std::vector<IndexInfo *> indexes;

  for (auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(name)) {
    if (index->table_name_ == name) {
      indexes.emplace_back(index);
    }
  }

  // why to insert into all child page ?
  Tuple child_tuple{};
  int num = 0;
  auto &&transaction = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&child_tuple, rid)) {
    ++num;
    exec_ctx_->GetCatalog()
        ->GetTable(plan_->TableOid())
        ->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());

    for (auto &index : indexes) {
      index->index_->InsertEntry(child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
                                                          *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
                                 *rid, transaction);
    }
  }
  std::vector<Value> values;
  values.emplace_back(Value(TypeId::INTEGER, num));
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
