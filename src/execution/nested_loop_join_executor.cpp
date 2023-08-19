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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  RID rid_holder{};
  right_executor_->Init();
  left_executor_->Init();
  right_tuple_ = Tuple{};
  left_tuple_ = Tuple{};
  right_status_ = false;
  left_status_ = left_executor_->Next(&left_tuple_, &rid_holder);
  left_join_found_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID rid_holder{};
  while (true) {
    right_status_ = right_executor_->Next(&right_tuple_, &rid_holder);
    if (!right_status_) {
      if (plan_->GetJoinType() == JoinType::LEFT && !left_join_found_) {
        std::vector<Value> val;
        auto right_column_count = right_executor_->GetOutputSchema().GetColumnCount();
        auto left_column_count = left_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < left_column_count; ++i) {
          val.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_column_count; ++i) {
          val.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(val, &plan_->OutputSchema());
        left_join_found_ = true;
        return true;
      }
      left_status_ = left_executor_->Next(&left_tuple_, &rid_holder);
      left_join_found_ = false;
      if (!left_status_) {
        return false;
      }
      right_executor_->Init();
      right_status_ = right_executor_->Next(&right_tuple_, &rid_holder);
      if (!right_status_) {
        continue;
      }
    }
    auto pred_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                      right_executor_->GetOutputSchema());
    if (!pred_value.IsNull() && pred_value.GetAs<bool>()) {
      std::vector<Value> val;
      auto right_column_count = right_executor_->GetOutputSchema().GetColumnCount();
      auto left_column_count = left_executor_->GetOutputSchema().GetColumnCount();
      for (uint32_t i = 0; i < left_column_count; ++i) {
        val.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_column_count; ++i) {
        val.emplace_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
      }
      *tuple = Tuple(val, &plan_->OutputSchema());
      left_join_found_ = true;
      return true;
    }
  }
  return false;
}
}  // namespace bustub
