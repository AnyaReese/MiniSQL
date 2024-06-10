#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"
#include "common/config.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

/** CheckPoint 结构体
 * 用于记录 CheckPoint 信息
 * LSN（Log Sequence Number）是用来标识事务日志记录中的位置的序列号。
 * ATT（Active Transaction Table）是一个活跃事务表，用于记录活跃事务的 ID 和最后一个 LSN。
 * KvDatabase 是一个键值对数据库，用于记录数据库中的数据。
 */
struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN}; // CheckPoint 的 LSN
    ATT active_txns_{}; // 活跃事务表
    KvDatabase persist_data_{}; // 数据库

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * DONE
    * Initialize the RecoveryManager with the last checkpoint.
    */
    void Init(CheckPoint &last_checkpoint) {
      data_ = last_checkpoint.persist_data_;
      active_txns_ = last_checkpoint.active_txns_;
      persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    }

    /**
    * TODO: Student Implement
    * 从 CheckPoint 开始，根据不同日志的类型对 KvDatabase 和活跃事务列表作出修改
    */
    void RedoPhase() {
      /** 这个循环从上一个已知的持久化 LSN（persist_lsn_）开始，
       * 一直到最新的LSN（LogRec::next_lsn_），
       * 遍历所有的日志记录。*/
      int i = persist_lsn_;
      for (; i < LogRec::next_lsn_; i++) {
        auto rec = log_recs_[i];
        active_txns_[rec->txn_id] = rec->lsn_; // 更新活跃事务列表
        switch (rec->type_) {
          case LogRecType::kInsert:
            data_[rec->new_key] = rec->new_value;
            break;
          case LogRecType::kDelete:
            data_.erase(rec->new_key);
            break;
          case LogRecType::kUpdate:
            data_.erase(rec->old_key);
            data_[rec->new_key] = rec->new_value;
            break;
          case LogRecType::kCommit:
            active_txns_.erase(rec->lsn_);
            break;
//          case LogRecType::kBegin:
//            active_txns_[rec->txn_id] = rec->lsn_;
//            break;
          /* Abort:
           * 回滚该事务之前的所有操作。
           * 使用while循环，通过prev_lsn_追溯至最初的日志记录，逐条回滚所有操作。
           * 对于插入操作，删除对应的键；对于删除操作，恢复键值对；对于更新操作，恢复到更新前的键值对状态。
           */
          case LogRecType::kAbort: {
            auto iter_rec = rec;
            while (iter_rec->prev_lsn_ != INVALID_LSN) {
              iter_rec = log_recs_[iter_rec->prev_lsn_];
              switch (iter_rec->type_) {
                case LogRecType::kInsert:
                  data_.erase(iter_rec->new_key);
                  break;
                case LogRecType::kDelete:
                  data_[iter_rec->new_key] = iter_rec->new_value;
                  break;
                case LogRecType::kUpdate:
                  data_.erase(iter_rec->new_key);
                  data_[iter_rec->old_key] = iter_rec->old_value;
                  break;
                default: break;
              } /* end of inner switch case */
            } /* end of inner while */
          break;
          } /* end of case LogRecType::kAbort */
          default: break;
        } /* end of switch case of redo */
      }
    }

    /**
    * DONE
    * 从活跃事务列表中找到所有活跃事务的最后一个日志记录，
    */
    void UndoPhase() {
      for (auto iter : active_txns_) {
        auto res = log_recs_.find(iter.second);
        while (res != log_recs_.end()) {
          auto rec = res->second;
          switch (rec->type_) {
            case LogRecType::kInsert:
              data_.erase(rec->new_key);
              break;
            case LogRecType::kDelete:
              data_[rec->old_key] = rec->old_value;
              break;
            case LogRecType::kUpdate:
              data_.erase(rec->new_key);
              data_[rec->old_key] = rec->old_value;
              break;
            default: break;
          }
          res = log_recs_.find(rec->prev_lsn_);
        }
      }
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
