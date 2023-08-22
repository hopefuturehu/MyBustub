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
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    //  X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    //  X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
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
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
}

auto LockManager::CheckPreLock(LockMode pre_lock, LockMode lock_mode) -> bool {
  // IS -> [S, X, IX, SIX]
  if (pre_lock == LockMode::INTENTION_SHARED) {
    return true;
  }
  //  S -> [X, SIX]
  if (pre_lock == LockMode::SHARED) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
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

void LockManager::ModTableLockSet(Transaction* txn, const std::shared_ptr<LockRequest>& request, bool is_insert){
  if(is_insert){
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


auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  assert(CheckLockValidity(txn, lock_mode));
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_que = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  lock_request_que->latch_.lock();
  bool is_upgrade = ChecktTableLockUpgrade(txn, lock_mode, oid);
  if (!is_upgrade) {
    lock_request_que->latch_.unlock();
    return true;
  }  // no need to upgrade lock, unlock and return
  if (lock_request_que->upgrading_ != INVALID_TXN_ID) {
    lock_request_que->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }  // only one transaction should be allowed to upgrade its lock on a given resource.
  
  for(const auto& requst:lock_request_que->request_queue_) {
    if(requst->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if(requst->lock_mode_ == lock_mode) {
      lock_request_que->latch_.unlock();
      return true;
    }
    if(!CheckPreLock(requst->lock_mode_, lock_mode)) {
      lock_request_que->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    lock_request_que->request_queue_.remove(requst);
    ModTableLockSet(txn, requst, false);

    lock_request_que->upgrading_ = txn->GetTransactionId();
    auto lock_quest = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request_que->InsertIntoQueue(lock_quest, is_upgrade);
  }
  



  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
