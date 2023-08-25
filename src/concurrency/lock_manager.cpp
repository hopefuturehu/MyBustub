//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::CheckLockValidity(Transaction *txn, LockMode lock_mode) -> bool {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    //  S, IS, SIX locks are never allowed in READ_UNCOMMITTED
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      LOG_DEBUG("txn %d made a unvailid LockTable Request of mode %d\n", txn->GetTransactionId(),
                static_cast<int>(lock_mode));
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    //  X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      LOG_DEBUG("txn %d made a unvailid LockTable Request of mode %d\n", txn->GetTransactionId(),
                static_cast<int>(lock_mode));
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    //  X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      LOG_DEBUG("txn %d made a unvailid LockTable Request of mode %d\n", txn->GetTransactionId(),
                static_cast<int>(lock_mode));
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      LOG_DEBUG("txn %d made a unvailid LockTable Request of mode %d\n", txn->GetTransactionId(),
                static_cast<int>(lock_mode));
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  return true;
}

auto LockManager::ChecktTableLockUpgrade(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // IS -> [S, X, IX, SIX]
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return lock_mode != LockMode::INTENTION_SHARED;
  }
  //  S -> [X, SIX]
  if (txn->IsTableSharedLocked(oid)) {
    if (lock_mode == LockMode::SHARED) {
      return false;
    }
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      return true;
    }
  }
  //  IX -> [X, SIX]
  if (txn->IsTableIntentionSharedLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      return false;
    }
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return true;
    }
  }
  //  SIX -> [X]
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return false;
    }
    if (lock_mode == LockMode::EXCLUSIVE) {
      return true;
    }
  }
  // No lock before
  if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedLocked(oid) &&
      !txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return false;
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
}

auto LockManager::CheckRowLockUpgrade(Transaction *txn, LockMode lock_mode, RID &rid) -> bool { return false; }
auto LockManager::CheckPreLock(LockMode pre_lock, LockMode lock_mode) -> bool {
  // IS -> [S, X, IX, SIX]
  if (pre_lock == LockMode::INTENTION_SHARED) {
    return true;
  }
  //  S -> [X, SIX]
  if (pre_lock == LockMode::SHARED) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return true;
    }
  }
  //  IX -> [X, SIX]
  if (pre_lock == LockMode::INTENTION_EXCLUSIVE) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return true;
    }
  }
  //  SIX -> [X]
  if (pre_lock == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return false;
    }
    if (lock_mode == LockMode::EXCLUSIVE) {
      return true;
    }
  }
  return false;
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &request, const std::shared_ptr<LockRequestQueue> &queue)
    -> bool {
  // return all_of(queue->request_queue_.begin(), queue->request_queue_.end(),
  //               [&](std::shared_ptr<LockRequest> &req) -> bool {
  //                 if(req->lock_mode_ == request->lock_mode_ ) {
  //                   return true;
  //                 }
  //                 return CheckPreLock(req->lock_mode_, request->lock_mode_) && req->granted_;
  //               });

  for (auto &lr : queue->request_queue_) {
    if (lr->granted_) {
      switch (request->lock_mode_) {
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}
void LockManager::ModTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &request, bool is_insert) {
  if (is_insert) {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(request->oid_);
    }
  } else {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->erase(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->erase(request->oid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->erase(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->erase(request->oid_);
    } else if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(request->oid_);
    }
  }
}

void LockManager::ModRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &request, bool is_insert) {
  if (is_insert) {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->operator[](request->oid_).insert(request->rid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->operator[](request->oid_).insert(request->rid_);
    }
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // LOG_DEBUG("txn %d made a LockTable Request on table %d of mode %d\n", txn->GetTransactionId(), oid,
  //           static_cast<int>(lock_mode));
  assert(CheckLockValidity(txn, lock_mode));
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_que = table_lock_map_[oid];
  lock_request_que->latch_.lock();
  table_lock_map_latch_.unlock();
  auto request_ptr = std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                                  [&](const std::shared_ptr<LockRequest> &request) -> bool {
                                    return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid;
                                  });
  bool is_upgrade = false;
  if (request_ptr != lock_request_que->request_queue_.end()) {
    is_upgrade = true;
    auto request = *request_ptr;
    if (request->lock_mode_ == lock_mode) {
      lock_request_que->latch_.unlock();
      return true;
    }  // 如果是相同锁，直接返回，不需要操作
    if (!CheckPreLock(request->lock_mode_, lock_mode)) {
      lock_request_que->latch_.unlock();
      // LOG_DEBUG("txn %d made a LockTable Request on table %d of mode %d aborted INCOMPATIBLE_UPGRADE\n",
      //           txn->GetTransactionId(), oid, static_cast<int>(lock_mode));
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }  //  如果不符合锁升级的条件，报错
    lock_request_que->request_queue_.erase(request_ptr);
    ModTableLockSet(txn, request, false);  // 移除旧的锁
  }
  if (lock_request_que->upgrading_ != INVALID_TXN_ID) {
    lock_request_que->latch_.unlock();
    // LOG_DEBUG("txn %d made a LockTable Request on table %d of mode %d aborted UPGRADE_CONFLICT\n",
    //           txn->GetTransactionId(), oid, static_cast<int>(lock_mode));
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }  // only one transaction should be allowed to upgrade its lock on a given resource.
  // 获取新的锁
  if (is_upgrade) {
    lock_request_que->upgrading_ = txn->GetTransactionId();
  }
  auto lock_quest = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_que->InsertIntoQueue(lock_quest, is_upgrade);
  std::unique_lock<std::mutex> lock(lock_request_que->latch_, std::adopt_lock);
  while (!GrantLock(lock_quest, lock_request_que)) {
    lock_request_que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      if (is_upgrade) {
        lock_request_que->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_que->request_queue_.remove(lock_quest);
      lock_request_que->cv_.notify_all();
      return false;
    }
  }
  lock_quest->granted_ = true;
  if (is_upgrade) {
    lock_request_que->upgrading_ = INVALID_TXN_ID;
  }
  lock_request_que->upgrading_ = INVALID_TXN_ID;
  ModTableLockSet(txn, lock_quest, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_que->cv_.notify_all();
  }
  lock.unlock();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // LOG_DEBUG("txn %d made a UnLockTable Request on table %d \n", txn->GetTransactionId(), oid);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // check row lock
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    // LOG_DEBUG("txn %d made a UnLockTable Request on table %d aborted for TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS\n",
    // txn->GetTransactionId(), oid);
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  auto requst =
      std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                   [&](const std::shared_ptr<LockRequest> &request) -> bool {
                     return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->granted_;
                   });
  if (requst != lock_request_queue->request_queue_.end()) {
    auto request = *requst;
    lock_request_queue->request_queue_.remove(request);
    lock_request_queue->cv_.notify_all();
    lock_request_queue->latch_.unlock();
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
         (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE)) {
      if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    ModTableLockSet(txn, request, false);
    return true;
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  // LOG_DEBUG("txn %d made a UnLockTable Request on table %d aborted for ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD\n",
  //           txn->GetTransactionId(), oid);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // LOG_DEBUG("txn %d made a LockRow Request on table %d row %s of mode %d\n", txn->GetTransactionId(), oid,
  //          rid.ToString().c_str(), static_cast<int>(lock_mode));
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  assert(CheckLockValidity(txn, lock_mode));
  if (lock_mode == LockMode::EXCLUSIVE) {
    // need X, IX, or SIX lock on table
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  } else if (lock_mode == LockMode::SHARED) {
    // any lock on table suffices
    if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }  // multilevel locking
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_que = row_lock_map_[rid];
  lock_request_que->latch_.lock();
  row_lock_map_latch_.unlock();
  bool is_upgrade = false;
  if (lock_request_que->upgrading_ != INVALID_TXN_ID) {
    lock_request_que->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }  // only one transaction should be allowed to upgrade its lock on a given resource.
  auto request = std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                              [&](const std::shared_ptr<LockRequest> &request) -> bool {
                                return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid;
                              });
  if (request != lock_request_que->request_queue_.end()) {
    is_upgrade = true;
    auto requst = *request;
    if (requst->lock_mode_ == lock_mode) {
      lock_request_que->latch_.unlock();
      return true;
    }
    if (!CheckPreLock(requst->lock_mode_, lock_mode)) {
      lock_request_que->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    lock_request_que->request_queue_.erase(request);
    if (requst->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
    } else if (requst->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
    }
  }
  if (is_upgrade) {
    lock_request_que->upgrading_ = txn->GetTransactionId();
  }
  auto lock_quest = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_que->InsertIntoQueue(lock_quest, true);
  std::unique_lock<std::mutex> lock(lock_request_que->latch_, std::adopt_lock);
  while (!GrantLock(lock_quest, lock_request_que)) {
    lock_request_que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      if (is_upgrade) {
        lock_request_que->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_que->request_queue_.remove(lock_quest);
      lock_request_que->cv_.notify_all();
      return false;
    }
  }
  if (lock_quest->lock_mode_ == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->operator[](lock_quest->oid_).insert(lock_quest->rid_);
  } else if (lock_quest->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->operator[](lock_quest->oid_).insert(lock_quest->rid_);
  }
  lock_quest->granted_ = true;
  if (is_upgrade) {
    lock_request_que->upgrading_ = INVALID_TXN_ID;
  }
  ModRowLockSet(txn, lock_quest, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_que->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // LOG_DEBUG("txn %d made a UnLockRow Request on table %d row %s \n", txn->GetTransactionId(), oid,
  //           rid.ToString().c_str());
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_que = row_lock_map_[rid];
  lock_request_que->latch_.lock();
  row_lock_map_latch_.unlock();
  auto request_ptr =
      std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                   [&](const std::shared_ptr<LockRequest> &request) -> bool {
                     return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == rid;
                   });
  if (request_ptr != lock_request_que->request_queue_.end()) {
    auto request = *request_ptr;
    lock_request_que->request_queue_.remove(request);
    lock_request_que->cv_.notify_all();
    lock_request_que->latch_.unlock();
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
    }
    lock_request_que->cv_.notify_all();
    lock_request_que->latch_.unlock();
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
         (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE)) {
      if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    return true;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lock_request_que->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // assume the graph is already fully built
  std::deque<txn_id_t> path;
  std::set<txn_id_t> visited;
  for (const auto &[start_node, end_node_set] : waits_for_) {
    if (visited.find(start_node) == visited.end()) {
      auto cycle_id = DFS(start_node, visited, path);
      if (cycle_id != -1) {
        // trim the path and retain only those involved in cycle
        auto it = std::find(path.begin(), path.end(), cycle_id);
        path.erase(path.begin(), it);
        std::sort(path.begin(), path.end());
        txn_id_t to_abort = path.back();
        *txn_id = to_abort;  // pick the youngest to abort
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[start_edge, end_edge_set] : waits_for_) {
    for (const auto &end_edge : end_edge_set) {
      edges.emplace_back(start_edge, end_edge);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock table_lock(table_lock_map_latch_);
      std::unique_lock row_lock(row_lock_map_latch_);
      LockManager::BuildGraph();
      txn_id_t abort_id = -1;
      while (HasCycle(&abort_id)) {
        waits_for_.erase(abort_id);
        for (auto &[start_node, end_node_set] : waits_for_) {
          end_node_set.erase(abort_id);
        }
        auto to_abort_ptr = TransactionManager::GetTransaction(abort_id);
        to_abort_ptr->SetState(TransactionState::ABORTED);
      }
      if (abort_id != -1) {
        for (const auto &[table_id, request_queue] : table_lock_map_) {
          request_queue->cv_.notify_all();
        }
        for (const auto &[row_id, request_queue] : row_lock_map_) {
          request_queue->cv_.notify_all();
        }
      }
    }
  }
}

}  // namespace bustub
