//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      l_exec_(std::move(left_executor)),
      r_exec_(std::move(right_executor)),
      l_match_(false),
      is_final_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw "only support left join and inner join";
  }
}

void NestedLoopJoinExecutor::Init() {
  r_exec_->Init();
  l_exec_->Init();
  RID rid;
  l_exec_->Next(&l_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 1. inner join
  if (plan_->GetJoinType() == JoinType::INNER) {
    Tuple r_tuple{};

    while (true) {
      if (!r_exec_->Next(&r_tuple, rid)) {
        r_exec_->Init();

        if (!l_exec_->Next(&l_tuple_, rid)) {
          return false;
        }
        r_exec_->Next(&r_tuple, rid);
      }
      auto value =
          plan_->Predicate().EvaluateJoin(&l_tuple_, l_exec_->GetOutputSchema(), &r_tuple, r_exec_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(l_exec_->GetOutputSchema().GetColumnCount() + r_exec_->GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < l_exec_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(l_tuple_.GetValue(&l_exec_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < r_exec_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(r_tuple.GetValue(&r_exec_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }
  }
  // 2. left join
  if (plan_->GetJoinType() == JoinType::LEFT) {
    Tuple r_tuple{};

    if (is_final_) {
      return false;
    }

    while (true) {
      if (!r_exec_->Next(&r_tuple, rid)) {
        r_exec_->Init();
        if (l_match_) {
          l_match_ = false;
          if (!l_exec_->Next(&l_tuple_, rid)) {
            return false;
          }
          r_exec_->Next(&r_tuple, rid);
        } else {
          std::vector<Value> values{};
          values.reserve(l_exec_->GetOutputSchema().GetColumnCount() + r_exec_->GetOutputSchema().GetColumnCount());
          for (uint32_t i = 0; i < l_exec_->GetOutputSchema().GetColumnCount(); ++i) {
            values.emplace_back(l_tuple_.GetValue(&l_exec_->GetOutputSchema(), i));
          }

          auto columns = r_exec_->GetOutputSchema().GetColumns();
          for (uint32_t i = 0; i < r_exec_->GetOutputSchema().GetColumnCount(); ++i) {
            values.emplace_back(ValueFactory::GetNullValueByType(columns[i].GetType()));
          }
          *tuple = Tuple{values, &GetOutputSchema()};

          if (!l_exec_->Next(&l_tuple_, rid)) {
            is_final_ = true;
          }
          return true;
        }
      }

      // merge the l_exec and r_exec results into tuple
      auto value =
          plan_->Predicate().EvaluateJoin(&l_tuple_, l_exec_->GetOutputSchema(), &r_tuple, r_exec_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(l_exec_->GetOutputSchema().GetColumnCount() + r_exec_->GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < l_exec_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(l_tuple_.GetValue(&l_exec_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < r_exec_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(r_tuple.GetValue(&r_exec_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        l_match_ = true;
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
