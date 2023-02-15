#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_exec_->Init();
  tuples_.clear();

  Tuple child_tuple{};
  RID child_rid{};

  while (child_exec_->Next(&child_tuple, &child_rid)) {
    tuples_.emplace_back(child_tuple, child_rid);
  }

  // sort by value
  std::sort(tuples_.begin(), tuples_.end(), [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) {
    auto order_bys = plan_->GetOrderBy();
    // can be sorted by mutiple fields
    for (auto &order_by : order_bys) {
      Value v_lhs = order_by.second->Evaluate(&lhs.first, child_exec_->GetOutputSchema());
      Value v_rhs = order_by.second->Evaluate(&rhs.first, child_exec_->GetOutputSchema());

      // cmp order:asc( defualt), desc
      // if not equal
      if (v_lhs.CompareEquals(v_rhs) != CmpBool::CmpTrue) {
        if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
          return v_lhs.CompareLessThan(v_rhs) == CmpBool::CmpTrue;
        }
        return v_lhs.CompareGreaterThan(v_rhs) == CmpBool::CmpTrue;
      }
    }
    // equal
    return false;
  });

  tuples_it_ = tuples_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // bug fix: tuples_.end( )
  if (tuples_it_ == tuples_.cend()) {
    return false;
  }

  *tuple = tuples_it_->first;
  *rid = tuples_it_->second;
  tuples_it_++;
  return true;
}

}  // namespace bustub
