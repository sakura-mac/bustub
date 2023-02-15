#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // get all tuples, and limit the n for Next to iterate
  // OPTIMISE OPTION: push all tuples into std::priority_queue with limited n_, Next function to pop while not empty
  child_exec_->Init();
  tuples_.clear();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_exec_->Next(&child_tuple, &child_rid)) {
    tuples_.emplace_back(child_tuple, child_rid);
  }

  std::make_heap(std::begin(tuples_), std::end(tuples_),
                 [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) {
                   auto order_bys = plan_->GetOrderBy();
                   for (auto &order_by : order_bys) {
                     Value v_lhs = order_by.second->Evaluate(&lhs.first, child_exec_->GetOutputSchema());
                     Value v_rhs = order_by.second->Evaluate(&rhs.first, child_exec_->GetOutputSchema());

                     if (v_lhs.CompareEquals(v_rhs) != CmpBool::CmpTrue) {
                       if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
                         return v_lhs.CompareGreaterThan(v_rhs) == CmpBool::CmpTrue;
                       }
                       return v_lhs.CompareLessThan(v_rhs) == CmpBool::CmpTrue;
                     }
                   }
                   return false;
                 });
  n_ = plan_->GetN();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (n_ == 0 || tuples_.empty()) {
    return false;
  }

  // pop the priority queue
  auto cmp = [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) {
    auto order_bys = plan_->GetOrderBy();
    for (auto &order_by : order_bys) {
      Value v_lhs = order_by.second->Evaluate(&lhs.first, child_exec_->GetOutputSchema());
      Value v_rhs = order_by.second->Evaluate(&rhs.first, child_exec_->GetOutputSchema());

      if (v_lhs.CompareEquals(v_rhs) != CmpBool::CmpTrue) {
        if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
          return v_lhs.CompareGreaterThan(v_rhs) == CmpBool::CmpTrue;
        }
        return v_lhs.CompareLessThan(v_rhs) == CmpBool::CmpTrue;
      }
    }
    return false;
  };
  std::pop_heap(tuples_.begin(), tuples_.end(), cmp);
  *tuple = tuples_.back().first;
  *rid = tuples_.back().second;
  tuples_.pop_back();
  n_--;
  return true;
}

}  // namespace bustub
