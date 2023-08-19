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
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      cursor_(aht_.Begin()),
      end_(aht_.End()),
      table_empty_(true) {}

void AggregationExecutor::Init() {
  aht_.Clear();
  child_->Init();
  Tuple child_tuple{};
  RID rid_holder{};
  bool status = child_->Next(&child_tuple, &rid_holder);
  while (status) {
    auto key = MakeAggregateKey(&child_tuple);
    auto val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, val);
    status = child_->Next(&child_tuple, &rid_holder);
  }
  cursor_ = aht_.Begin();
  end_ = aht_.End();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto return_schema = plan_->OutputSchema();
  if (cursor_ == end_) {
    if (cursor_ == aht_.Begin() && table_empty_) {
      if (!plan_->GetGroupBys().empty()) {
        return false;
      }
      auto val = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(val.aggregates_, &return_schema);
      table_empty_ = false;
      return true;
    }
    return false;
  }
  auto key = cursor_.Key();
  auto val = cursor_.Val();
  std::copy(val.aggregates_.begin(), val.aggregates_.end(), std::back_inserter(key.group_bys_));
  ++cursor_;
  *tuple = Tuple(key.group_bys_, &return_schema);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
