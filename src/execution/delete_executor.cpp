//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), is_deleted_(false) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  is_deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  is_deleted_ = true;

  // add rreferenc is ok?
  auto &&table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::string name = table_info->name_;
  std::vector<IndexInfo *> indexes;

  for (auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(name)) {
    if (index->table_name_ == name) {
      indexes.emplace_back(index);
    }
  }

  // mark the delete rid instead of delete directly
  Tuple child_tuple{};
  int num = 0;
  auto &&transaction = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&child_tuple, rid)) {
    ++num;
    table_info->table_->MarkDelete(*rid, transaction);

    for (auto &index : indexes) {
      index->index_->DeleteEntry(child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
                                                          *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
                                 *rid, transaction);
    }
  }

  std::vector<Value> values{};
  values.emplace_back(Value(TypeId::INTEGER, num));
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
