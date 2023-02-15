//===----------------------------------------------------------------------===//
//
//                         BusTub
// // seq_scan_executor.cpp //
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

// 1. exec_ctx provide the context resources
// 2. plan decides which table to scan
// 3. table page stored as 2way lists in table heap, tuple contains the RID( page id and slot num) and value( raw value
// and metadata)
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      exec_ctx_(exec_ctx),
      table_it_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() {
  table_it_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_it_ == exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
    return false;
  }

  *tuple = *table_it_;
  *rid = (*tuple).GetRid();
  ++table_it_;

  return true;
}

}  // namespace bustub
