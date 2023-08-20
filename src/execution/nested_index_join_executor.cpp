//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      inner_schema_(plan_->InnerTableSchema()),
      outer_schema_(plan->GetChildPlan()->OutputSchema()),
      key_schema_(std::vector<Column>{{"index_key", plan->KeyPredicate()->GetReturnType()}}) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  inner_table_ptr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  match_rids_.clear();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    bool outer_status = child_executor_->Next(&outer_tuple_, &outer_rid_);
    if (!outer_status) {
      return false;
    }
    std::vector<Value> key_val{plan_->KeyPredicate()->Evaluate(&outer_tuple_, outer_schema_)};
    Tuple key_index = Tuple(key_val, &key_schema_);
    index_->index_->ScanKey(key_index, &match_rids_, exec_ctx_->GetTransaction());
    if (!match_rids_.empty()) {
      inner_rid_ = match_rids_.back();
      match_rids_.clear();
      inner_table_ptr_->table_->GetTuple(inner_rid_, &inner_tuple_, exec_ctx_->GetTransaction());
      std::vector<Value> val;
      uint32_t inner_column = inner_schema_.GetColumnCount();
      uint32_t outer_column = outer_schema_.GetColumnCount();
      for (uint32_t i = 0; i < outer_column; ++i) {
        val.emplace_back(outer_tuple_.GetValue(&outer_schema_, i));
      }
      for (uint32_t i = 0; i < inner_column; ++i) {
        val.emplace_back(inner_tuple_.GetValue(&inner_schema_, i));
      }
      *tuple = Tuple(val, &GetOutputSchema());
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> val;
      uint32_t inner_column = plan_->InnerTableSchema().GetColumnCount();
      uint32_t outer_column = plan_->GetChildPlan()->OutputSchema().GetColumnCount();
      for (uint32_t i = 0; i < outer_column; ++i) {
        val.emplace_back(outer_tuple_.GetValue(&plan_->GetChildPlan()->OutputSchema(), i));
      }
      for (uint32_t i = 0; i < inner_column; ++i) {
        val.emplace_back(ValueFactory::GetNullValueByType(inner_schema_.GetColumn(i).GetType()));
      }
      *tuple = Tuple(val, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
