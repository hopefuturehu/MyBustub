#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  sorted_.clear();
  size_t n = plan_->GetN();
  Tuple child_tuple{};
  RID child_rid{};
  auto cmp = [&](const Tuple &a, const Tuple &b) -> bool {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  auto status = child_executor_->Next(&child_tuple, &child_rid);
  while (status) {
    if (pq.size() < n) {
      pq.push(child_tuple);
    } else {
      if (cmp(child_tuple, pq.top())) {
        pq.pop();
        pq.push(child_tuple);
      }
    }
    status = child_executor_->Next(&child_tuple, &child_rid);
  }
  sorted_.reserve(n);
  while (!pq.empty()) {
    sorted_.emplace_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  const auto &next_tuple = sorted_.back();
  *tuple = next_tuple;
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
