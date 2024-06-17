#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId &rid, Txn *txn, Row *row) {
  this->table_heap_ = table_heap;
  this->rid = rid;
  this->txn = txn;
  if (row) {
    this->row_ = new Row(*row);
  }
  else this->row_ = nullptr;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid = other.rid;
  txn = other.txn;
  if (other.row_) {
    row_ = new Row(*other.row_);
  }
  else row_ = nullptr;
}

TableIterator::~TableIterator() {
  if(row_ != nullptr) delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return itr.rid == rid;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(itr.rid == rid);
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (itr.row_) {
    row_ = new Row(*itr.row_);
  }
  else row_ = nullptr;
  table_heap_ = itr.table_heap_;
  rid = itr.rid;
  txn = itr.txn;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // 首先，要获取当前元组所在的磁盘页
  ASSERT(row_ != nullptr, "ERROR: do \"++\" operation on a null iterator is wrong");//如果row_ == nullptr，则报错
  page_id_t page_id = rid.GetPageId();
  ASSERT(page_id != INVALID_PAGE_ID, "ERROR: do \"++\" operation on end iterator is wrong");//如果page_id == INVALID，说明已经到结尾，报错
  auto *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  ASSERT(page_id == page->GetPageId(), "ERROR: \"page_id == page->GetPageId()\" should be true");//简单判断一下
  RowId nextid;//准备存储下一个rowid
  if (page->GetNextTupleRid(rid, &nextid)) {
    row_->GetFields().clear();//将row的field清空，准备存储下一个row
    rid.Set(nextid.GetPageId(), nextid.GetSlotNum());//将rid设置成下一个row的id
    row_->SetRowId(rid);//将row设置成下一个row
    table_heap_->GetTuple(row_, nullptr);//获取下一个row
    row_->SetRowId(rid);
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  }
  //如果获取下一个row失败，则可能是当前页已经读到最后一个row，需要读取下一页
  page_id_t next_page_id = INVALID_PAGE_ID;
  while ((next_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {//持续获取有效的下一页，直到在该页可以得到元组
    auto *next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    page = next_page;
    if (page->GetFirstTupleRid(&nextid)) {//获取首个元组，若失败，则继续循环
      row_->GetFields().clear();
      rid = nextid;
      row_->SetRowId(rid);
      table_heap_->GetTuple(row_, nullptr);
      row_->SetRowId(rid);
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return *this;
    }
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  // ++失败
  rid.Set(INVALID_PAGE_ID, 0);//rid设置无效页
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  //前三行先获取当前row的信息，以供返回
  Row* row_next = new Row(*(this->row_));
  TableHeap* this_heap_next = this->table_heap_;
  RowId rid_next = this->rid;
  ++(*this);//再调用++iter重载函数
  return TableIterator(this_heap_next, rid_next, nullptr, row_next);
}
