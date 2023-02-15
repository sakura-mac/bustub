//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_exec_->Init();
  tuples_.clear();

  Tuple child_tuple{};
  RID child_rid{};

  // limit the number of pair stored in tuples
  for (size_t i = 0; i < plan_->GetLimit(); ++i) {
    if (!child_exec_->Next(&child_tuple, &child_rid)) {
      break;
    }
    tuples_.emplace_back(child_tuple, child_rid);
  }
  // why is cbegin : means should not change the value
  tuples_it_ = tuples_.cbegin();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_it_ == tuples_.cend()) {
    return false;
  }

  *tuple = tuples_it_->first;
  *rid = tuples_it_->second;
  tuples_it_++;
  return true;
}

}  // namespace bustub
