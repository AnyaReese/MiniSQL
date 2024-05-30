#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * DONE
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId &rid, Txn *txn, Row *row) {
  this->table_heap_ = table_heap;
  this->row_id_ = rid;
  this->txn_ = txn;
  if (row) {
    this->row_ = new Row(*row);
  }
  else this->row_ = nullptr;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_id_ = other.row_id_;
  txn_ = other.txn_;
  if (other.row_) {
    row_ = new Row(*other.row_);
  }
  else row_ = nullptr;
}


TableIterator::~TableIterator() {
  if(row_ != nullptr) delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row_id_ == itr.row_id_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(row_id_ == itr.row_id_);
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    row_id_ = itr.row_id_;
    row_ = itr.row_;
    txn_ = itr.txn_;
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  FindNextValidRow();
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  Row* this_row_ = new Row(*(this->row_));
  TableHeap* this_heap_ = this->table_heap_;
  RowId this_rid_ = this->row_id_;
  ++(*this);
  return TableIterator(this_heap_, this_rid_, nullptr, this_row_);
}

void TableIterator::FindNextValidRow() {
  while (!(row_id_ == RowId(-1, -1))) {
    // 获取当前页面
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_id_.GetPageId()));
    if (page == nullptr) {
      row_id_ = RowId(-1, -1);
      break;
    }

    // 获取页面的下一个页面ID
    page_id_t next_page_id = page->GetNextPageId();
    
    // 查找当前页面中的下一个元组
    bool found = page->GetNextTupleRid(row_id_, &row_id_);

    // 释放当前页面
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);

    // 如果找到有效的元组，更新row_并返回
    if (found) {
      row_->SetRowId(row_id_);
      if (table_heap_->GetTuple(row_, txn_)) {
        return;
      }
    } 
    // 如果未找到有效元组，但有下一页面，继续查找
    else if (next_page_id != INVALID_PAGE_ID) {
      row_id_ = RowId(next_page_id, 0);
    } 
    // 如果没有下一页面，标记row_id_为无效
    else {
      row_id_ = RowId(-1, -1);
    }
  }
}