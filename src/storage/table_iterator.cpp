#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), current_row_id_(rid), txn_(txn) {
    current_row_.SetRowId(current_row_id_);
    if (!table_heap_->GetTuple(&current_row_, txn_)) {
      FindNextValidRow();
    }
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_), current_row_id_(other.current_row_id_), current_row_(other.current_row_), txn_(other.txn_) {}


bool TableIterator::operator==(const TableIterator &itr) const {
  return current_row_id_ == itr.current_row_id_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return current_row_;
}

Row *TableIterator::operator->() {
  return &current_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    current_row_id_ = itr.current_row_id_;
    current_row_ = itr.current_row_;
    txn_ = itr.txn_;
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  FindNextValidRow();
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator tmp = TableIterator(*this);
  ++(*this);
  return tmp;
}

void TableIterator::FindNextValidRow() {
  while (!(current_row_id_ == RowId(-1, -1))) {
    // 获取当前页面
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(current_row_id_.GetPageId()));
    if (page == nullptr) {
      current_row_id_ = RowId(-1, -1);
      break;
    }

    // 获取页面的下一个页面ID
    page_id_t next_page_id = page->GetNextPageId();
    
    // 查找当前页面中的下一个元组
    bool found = page->GetNextTupleRid(current_row_id_, &current_row_id_);

    // 释放当前页面
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);

    // 如果找到有效的元组，更新current_row_并返回
    if (found) {
      current_row_.SetRowId(current_row_id_);
      if (table_heap_->GetTuple(&current_row_, txn_)) {
        return;
      }
    } 
    // 如果未找到有效元组，但有下一页面，继续查找
    else if (next_page_id != INVALID_PAGE_ID) {
      current_row_id_ = RowId(next_page_id, 0);
    } 
    // 如果没有下一页面，标记current_row_id_为无效
    else {
      current_row_id_ = RowId(-1, -1);
    }
  }
}