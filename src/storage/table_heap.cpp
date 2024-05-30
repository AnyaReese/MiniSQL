#include "storage/table_heap.h"



  // BufferPoolManager *buffer_pool_manager_;
  // page_id_t first_page_id_;
  // Schema *schema_;
  // [[maybe_unused]] LogManager *log_manager_;
  // [[maybe_unused]] LockManager *lock_manager_;


/**
 * DONE
 *       向堆表中插入一条记录，插入记录后生成的RowId需要通过row对象返回（即row.rid_）
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // Iterate over the pages to find a page with enough space
  page_id_t current_page_id = first_page_id_;
  while (current_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(current_page_id, true);
      return true;
    }
    current_page_id = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  }

  // No space available in existing pages, need to allocate a new page
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(current_page_id));
  if (new_page == nullptr) {
    return false;
    LOG(ERROR) << "Allocation failed" << std::endl;
  }

  new_page->Init(current_page_id, INVALID_PAGE_ID, log_manager_, txn);
  first_page_id_ = current_page_id;

  if (!new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), false);
    return false; // Insert failed even on new page
  }

  buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), true);
  return true;
}


bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * DONE
 *       将RowId为rid的记录old_row替换成新的记录new_row，并将new_row的RowId通过new_row.rid_返回；
 */
bool TableHeap::UpdateTuple(Row &new_row, const RowId &rid, Txn *txn) {
  // Find the page which contains the old tuple
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false; // Page not found
  }

  Row old_row;
  old_row.SetRowId(rid);

  // Fetch the old row from the page
  if (!page->GetTuple(&old_row, schema_, txn, lock_manager_)) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false; // Failed to fetch old tuple
  }

  // Call UpdateTuple with the correct parameters
  bool result = page->UpdateTuple(new_row, &old_row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), result);
  return result;
}

/**
 * DONE
 *       从物理意义上删除这条记录
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(ERROR) << "Allocation failed" << std::endl;
    return; // Page not found
  }

  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * DONE
 *       获取RowId为row->rid_的记录
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  // Find the page which contains the tuple
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    LOG(ERROR) << "Allocation failed" << std::endl;
    return false; // Page not found
  }

  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return result;
}


void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * DONE
 */
TableIterator TableHeap::Begin(Txn *txn) {
  // Start from the first page
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    return End(); // No pages in the table
  }

  // Find the first valid tuple
  RowId first_row_id;
  if (!page->GetFirstTupleRid(&first_row_id)) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return End(); // No valid tuples in the table
  }

  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return TableIterator(this, first_row_id, txn);
}

/**
 * DONE
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, RowId(-1, -1), nullptr);
}