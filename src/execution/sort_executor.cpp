#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  child_.clear();
  bool status = child_executor_->Next(&child_tuple, &child_rid);
  while (status) {
    child_.emplace_back(child_tuple);
    status = child_executor_->Next(&child_tuple, &child_rid);
  }
  std::sort(child_.begin(), child_.end(), [&](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &[type, expr] : plan_->GetOrderBy()) {
      auto a_val = expr->Evaluate(&a, plan_->OutputSchema());
      auto b_val = expr->Evaluate(&b, plan_->OutputSchema());
      if (a_val.CompareEquals(b_val) == CmpBool::CmpTrue) {
        continue;
      }
      auto cmp = a_val.CompareLessThan(b_val);
      if (cmp == CmpBool::CmpTrue && type == OrderByType::DESC) {
        return true;
      }
      if (cmp == CmpBool::CmpFalse && type != OrderByType::DESC) {
        return true;
      }
      return false;
    }
    assert(false);
  });
  // std::cout<<"sort init success!\n";
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_.empty()) {
    return false;
  }
  const auto &res_tuple = child_.back();
  *tuple = res_tuple;
  child_.pop_back();
  return true;
}

}  // namespace bustub
