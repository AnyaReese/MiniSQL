#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
// 总的来说，这个函数的主要作用是在事务请求 RID 的共享锁时，进行一系列的检查和操作，以确保事务能够安全地获取锁，并且在发生死锁时能够正确地处理。
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // 准备并检查锁，检查的当前事务是否处于收缩状态以及对于此rid是否有一个锁请求队列
  LockPrepare(txn, rid);

  // 如果隔离级别是ReadUncommitted，则Aborted，并抛出异常
  // 因为在这种隔离级别下，允许事务看到其他未提交事务的修改，事务不需要获取共享锁就可以读取数据
  if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }

  // 从锁表获取该对于rowid的锁请求队列
  LockRequestQueue &req_queue = lock_table_[rid];

  // 向该锁请求队列中添加该事务所请求的共享锁
  req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);

  // 检查是否有事务正在对该rowid进行写操作
  if (req_queue.is_writing_) {
    // 如果有，则当前事务阻塞，并等待lamnda函数的返回，如果当前事务被赋予Abort的状态或者没有事务对该rowid进行写操作，则返回true
    // 当前线程继续
    req_queue.cv_.wait(
        lock, [&req_queue, txn]() -> bool { return txn->GetState() == TxnState::kAborted || !req_queue.is_writing_; });
  }

  // 检查当前事务是否是因为死锁而被设置成abort状态，如果是，则抛出异常；不是，则继续下一步
  CheckAbort(txn, req_queue);

  // 否则，将该rowid添加到事务的共享锁合集
  txn->GetSharedLockSet().emplace(rid);
  ++req_queue.sharing_cnt_;

  // 将当前事务在锁请求队列中的锁请求状态设置为已授予共享锁
  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kShared;

  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  // 准备并检查锁
  LockPrepare(txn, rid);

  // 获取锁请求队列
  LockRequestQueue &req_queue = lock_table_[rid];

  // 向该锁请求队列中添加该事务所请求的独占锁
  req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);

  //  等待所有的对于该rowid的写请求和读请求全部结束，才会给该事务分配独占锁
  if (req_queue.is_writing_ || req_queue.sharing_cnt_ > 0) {
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool {
      return txn->GetState() == TxnState::kAborted || (!req_queue.is_writing_ && 0 == req_queue.sharing_cnt_);
    });
  }

  // 检查当前事务是否是因为死锁而被设置成abort状态，如果是，则抛出异常；不是，则继续下一步
  CheckAbort(txn, req_queue);

  // 将该 rid 添加到事务的独占锁集合中，并将锁请求队列标记为正在写入。
  txn->GetExclusiveLockSet().emplace(rid);
  req_queue.is_writing_ = true;

  //  将当前事务在锁请求队列中的锁请求状态设置为已授予独占锁
  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kExclusive;

  return true;
}

/**
 * TODO: Student Implement
 */
// 将事务（txn）在指定的记录（rid）上的锁从共享锁升级为独占锁
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // 准备并检查锁，检查的当前事务是否处于收缩状态以及对于此rid是否有一个锁请求队列
  LockPrepare(txn, rid);

  // 获取锁请求队列
  LockRequestQueue &req_queue = lock_table_[rid];

  // 如果当前有其他事务正在尝试升级锁，为了防止死锁，那么当前事务会被设置为 Aborted 状态，并抛出异常。
  if (req_queue.is_upgrading_) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
  }

  // 获取锁请求迭代器
  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());

  //  否则更改其请求锁模式为独占锁，授予锁模式为共享锁
  iter->lock_mode_ = LockMode::kExclusive;
  iter->granted_ = LockMode::kShared;

  //  如果当前有其他事务正在对该 rid 进行写操作，或者有其他事务持有该 rid 的共享锁，那么当前事务会被阻塞，直到所有其他事务的写请求和读请求都完成
  if (req_queue.is_writing_ || req_queue.sharing_cnt_ > 1) {
    // 此时，表示该锁请求队列中有事务正在进行锁升级
    req_queue.is_upgrading_ = true;
    // 等待直到lambda函数返回true
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool {
      return txn->GetState() == TxnState::kAborted || (!req_queue.is_writing_ && 1 == req_queue.sharing_cnt_);
    });
  }

  //  如果abort，则取消升级
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.is_upgrading_ = false;
  }

  // 如果因为死锁而abort，则抛出异常
  CheckAbort(txn, req_queue);

  // 从事务的共享锁集合中删除 rid，并减少共享锁数目
  txn->GetSharedLockSet().erase(rid);
  --req_queue.sharing_cnt_;

  // 将 rid 添加到事务的独占锁集合
  txn->GetExclusiveLockSet().emplace(rid);

  // 取消升级状态
  req_queue.is_upgrading_ = false;

  // 设置写入标志
  req_queue.is_writing_ = true;

  // 将该事务已授予的锁模式改为独占模式
  iter->granted_ = LockMode::kExclusive;

  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // 准备并检查锁，检查的当前事务是否处于收缩状态以及对于此rid是否有一个锁请求队列
  LockPrepare(txn, rid);

  LockRequestQueue &req_queue = lock_table_[rid];

  // 获取当前事务的锁模式
  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());
  auto lock_mode = iter->granted_;

  // 从该事务的共享锁和独占锁集合中移除rid
  if(lock_mode == LockMode::kShared)
    txn->GetSharedLockSet().erase(rid);
  else
    txn->GetExclusiveLockSet().erase(rid);

  //  从锁请求队列中移除当前事务的锁请求
  req_queue.EraseLockRequest(txn->GetTxnId());

  // 如果事务是活跃状态，并且他不是readcommitted隔离级别或他不是共享锁，则将事务设置为收缩状态
  // 因为在 ReadCommitted 隔离级别下，事务在读取数据时获取共享锁，读取完成后立即释放共享锁。所以，
  // 如果事务的隔离级别是 ReadCommitted 并且正在解锁的是共享锁，那么事务可以继续保持在 kGrowing 状态，因为它可能会继续获取新的共享锁来读取其他数据
  if (txn->GetState() == TxnState::kGrowing &&
      !(txn->GetIsolationLevel() == IsolationLevel::kReadCommitted && LockMode::kShared == lock_mode)) {
    txn->SetState(TxnState::kShrinking);
  }

  // 根据不同的锁类型来更新当前rid的锁申请队列的状态
  if (LockMode::kShared == lock_mode) {
    --req_queue.sharing_cnt_;
    req_queue.cv_.notify_all();
  } else {
    req_queue.is_writing_ = false;
    req_queue.cv_.notify_all();
  }

  return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  // 如果当前事务处于收缩状态，则他不能申请锁，则抛出异常
  if (txn->GetState() == TxnState::kShrinking) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  // 检查锁表中是否已经存在对应id的锁请求队列，如果不存在，则在锁表中创建一个新的锁请求队列
  if (lock_table_.find(rid) == lock_table_.end()) {
    // emplace插入会确保重复元素不会插入
    // std::piecewise_construct：这是一个标记类型，用于指示 emplace 方法构造键值对时，应该分别构造键和
    // 而不是使用默认的拷贝或移动构造函数。这允许我们为键和值提供独立的参数集。
    // std::forward_as_tuple(rid)：这是使用 std::forward_as_tuple 函数创建一个包含 rid 的元组。这个元组将被用作构造键的参数。
    // std::forward_as_tuple()：这是使用 std::forward_as_tuple 函数创建一个空的元组。这个元组将被用作构造值的参数。
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  // 如果该事务的状态为Aborted，则删除该事务在锁请求队列中的锁请求，并抛出异常，表示事务由于死锁而终止
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.EraseLockRequest(txn->GetTxnId());
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
  }
}

/**
 * TODO: Student Implement
 */
// 用于添加一条从t1到t2的边，表示t1等待t2释放锁
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
// 用于删除一条从t1到t2的边，表示t1不再等待t2释放锁
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(t2);
}

/**
 * TODO: Student Implement
 */
// 用于判断等待图中是否存在循环，如果存在，则返回true，并将循环中最新的事务id存储在newest_tid_in_cycle中
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  //  重置用于DFS的变量
  revisited_node_ = INVALID_TXN_ID; // 被初始化特殊值，表示未发现循环
  visited_set_.clear(); // 清空，用于DFS中存储访问过的点
  std::stack<txn_id_t>().swap(visited_path_);// 清空，用于DFS入栈出栈

  //  遍历等待图，将所有事物添加到txn_set中
  std::set<txn_id_t> txn_set;// 定义一个set，确保元素不重复，且按照id从小到大排序
  for (const auto &[t1, vec] : waits_for_) {
    txn_set.insert(t1);// 存储该事物
    for (const auto &t2 : vec) {// 存储该事务的相邻事务
      txn_set.insert(t2);
    }
  }

  //  对于txn_set中的每个事务，从小到大操作，寻找循环
  for (const auto &start_txn_id : txn_set) {
    if (DFS(start_txn_id)) {
      newest_tid_in_cycle = revisited_node_;// 如果找到了循环，则记录那个被访问两次的事务
      // 此循环是在做从重复元素进行循环前推，找到循环中id最大的事务，即最新创建的事务
      // 这里如果visited_path为空或revisited_node_==栈顶元素，则说明当前点即是循环起点，无需再做寻找
      while (!visited_path_.empty() && revisited_node_ != visited_path_.top()) {
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      // 找到的循环是第一个循环，找到的事务一定是循环中id最大的
      return true;
    }
  }

  // 如果无循环，则存储特殊id并返回false
  newest_tid_in_cycle = INVALID_TXN_ID;
  return false;
}

bool LockManager::DFS(txn_id_t txn_id) {
  if (visited_set_.find(txn_id) != visited_set_.end()) {// 判断该事务是否被访问过
    revisited_node_ = txn_id;// 如果被访问过，则存储该事务并返回true
    return true;
  }

  //  否则，将当前事务加到已访问节点中，并将其压入栈
  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);

  //  递归的访问当前事务的所有等待事务，判断他们是否已经被访问过
  for (const auto wait_for_txn_id : waits_for_[txn_id]) {
    if (DFS(wait_for_txn_id)) {
      return true;
    }
  }

  //  如果当前事务的所有等待事务都未检测到环，则弹出该事务并不再记录，并返回false
  visited_set_.erase(txn_id);
  visited_path_.pop();
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id: txn->GetSharedLockSet()) {
    for (const auto &lock_req: lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id: txn->GetExclusiveLockSet()) {
    for (const auto &lock_req: lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

/**
 * TODO: Student Implement
 */
/*总的来说，这个函数的主要作用是定期检测并处理死锁。它通过构建一个等待图，然后在图中寻找循环来检测死锁。一旦发现死锁，它就会选择一个事务进行中止，以解决死锁。*/
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) { // 当该变量为true，才执行后面的程序
    std::this_thread::sleep_for(cycle_detection_interval_);// 每次循环开始，线程会先进行一段时间的休眠
    {
      std::unique_lock<std::mutex> l(latch_);// 然后函数获得一个锁表，用来保护对锁表和等待图的访问
      std::unordered_map<txn_id_t, RowId> required_rec;// 用于存储事务id和他在锁表中对应的rowid
      //  建立一个等待图
      for (const auto &[row_id, lock_req_queue] : lock_table_) {// 遍历锁表中的每一个锁请求队列
        for (const auto &lock_req : lock_req_queue.req_list_) {// 遍历锁请求队列中的每一个锁请求
          if (lock_req.granted_ != LockMode::kNone) // 如果这个锁请求被授予了任何锁，则跳过
            continue;
          // 如果没有被授予任何锁，才进行后面的步骤
          required_rec[lock_req.txn_id_] = row_id; // 先存储该事务想要对哪个rowid申请锁
          for (const auto &granted_req : lock_req_queue.req_list_) {
            // 对于每一个在请求队列中的有被授予锁的事务，加一条从该事务到这些事务的边，以表示lock_req需要等待这些事务释放锁
            if (LockMode::kNone == granted_req.granted_)
              continue;
            AddEdge(lock_req.txn_id_, granted_req.txn_id_);
          }
        }
      }
      //  处理循环
      txn_id_t txn_id = INVALID_TXN_ID;
      // 如果HasCycle判断当前等待图存在死锁
      while (HasCycle(txn_id)) {
        // 获取到了txn_id, 即表示循环起始点（最年轻的事务）
        auto *txn = txn_mgr_->GetTransaction(txn_id);// 获取该事务
        DeleteNode(txn_id);// 从等待图中删除该事务
        txn->SetState(TxnState::kAborted);// 将该事务状态设置为Aborted
        lock_table_[required_rec[txn_id]].cv_.notify_all();// 获取 想要获取该rowid获取锁的所有在等待txn_id这个事务释放锁的 事务，将他们唤醒
      }
      waits_for_.clear();// 处理了所有循环之后，清除堆表
    }
  }
}

/**
 * TODO: Student Implement
 */
/*总的来说，这个函数的主要作用是获取等待图中所有边的列表*/
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (const auto &[t1, sibling_vec] : waits_for_) {// 遍历所有等待图中的点及其相邻点的集合
    for (const auto &t2 : sibling_vec) {// 遍历其相邻点，想result中添加这对点作为一条边
      result.emplace_back(t1, t2);
    }
  }
  std::sort(result.begin(), result.end());// 对这些边进行排序，按照两个点类似于字典序进行排序
  return result;
}
