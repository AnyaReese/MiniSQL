#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid, // invalid log record
    kInsert, // insert log record
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};

    /** New data members */
    [[maybe_unused]] KeyType old_key;
    [[maybe_unused]] ValType old_value;
    [[maybe_unused]] KeyType new_key;
    [[maybe_unused]] ValType new_value;
    txn_id_t txn_id; // transaction id

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * DONE
 * CreateInsertLog
 * @param ins_key insert key
 * @param ins_val insert value
 * @param txn_id transaction id
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  LogRec rec;
  rec.type_ = LogRecType::kInsert;
  rec.lsn_ = LogRec::next_lsn_;
  rec.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  LogRec::next_lsn_++;

  /** Initialize new data members */
  rec.new_key = ins_key;
  rec.new_value = ins_val;
  LogRecPtr ptr = std::make_shared<LogRec>(rec);
  return ptr;
}

/**
 * DONE
 * CreateDELETELog
 * @param del_key delete key
 * @param del_val delete value
 * @param txn_id transaction id
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  LogRec rec;
  rec.type_ = LogRecType::kDelete;
  rec.lsn_ = LogRec::next_lsn_;
  rec.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  LogRec::next_lsn_++;

  /** Initialize new data members */
  rec.new_key = del_key;
  rec.new_value = del_val;
  LogRecPtr ptr = std::make_shared<LogRec>(rec);
  return ptr;
}

/**
 * DONE
 * CreateUpdateLog
 * @param old_key old key
 * @param old_val old value
 * @param new_key new key
 * @param new_val new value
 * @param txn_id transaction id
 * @return LogRecPtr
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  LogRec rec;
  rec.type_ = LogRecType::kUpdate;
  rec.lsn_ = LogRec::next_lsn_;
  rec.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  LogRec::next_lsn_++;

  /** Initialize new data members */
  rec.old_key = old_key;
  rec.old_value = old_val;
  rec.new_key = new_key;
  rec.new_value = new_val;
  LogRecPtr ptr = std::make_shared<LogRec>(rec);
  return ptr;
}

/**
 * DONE
 * CreateBeginLog
 * @param txn_id transaction id
 * @return LogRecPtr
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    LogRec rec;
    rec.type_ = LogRecType::kBegin;
    rec.lsn_ = LogRec::next_lsn_;
//    rec.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    rec.prev_lsn_ = INVALID_LSN; // INVALID_LSN = -1
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    LogRec::next_lsn_++;
    rec.txn_id = txn_id;
    LogRecPtr ptr = std::make_shared<LogRec>(rec);
    return ptr;
}

/**
 * DONE
 * CreateCommitLog
 * @param txn_id transaction id
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    LogRec rec;
    rec.type_ = LogRecType::kCommit;
    rec.lsn_ = LogRec::next_lsn_;
    rec.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    LogRec::next_lsn_++;
    rec.txn_id = txn_id;
    LogRecPtr ptr = std::make_shared<LogRec>(rec);
    return ptr;
}

/**
 * DONE
 * CreateAbortLog
 * @param txn_id transaction id
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  LogRec rec;
  rec.type_ = LogRecType::kAbort;
  rec.lsn_ = LogRec::next_lsn_;
  rec.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  LogRec::next_lsn_++;
  rec.txn_id = txn_id;
  LogRecPtr ptr = std::make_shared<LogRec>(rec);
  return ptr;
}

#endif  // MINISQL_LOG_REC_H
