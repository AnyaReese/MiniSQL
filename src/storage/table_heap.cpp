#include "storage/table_heap.h"

/**
 * Done
 * 1. 从第一页开始插入
 * 2. 如果插入失败，新建一页
 * 3. 如果新建失败，返回 false
 * 4. 否则，插入成功，返回 true
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  bool is_not_valid = buffer_pool_manager_->IsPageFree(GetFirstPageId());
  if(is_not_valid) {//当判断出首页不可用，则新建首页
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
  }
  if(page == nullptr)//如果创建失败，返回false
  {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return false;
  }
  page_num = 1;
  page_id_t page_id = GetFirstPageId();//获取首页id
  while(1)
  {
    //尝试将row插入当前页，若成功，则返回true
    if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
    {
      return buffer_pool_manager_->UnpinPage(page_id, true);
    }
    //若失败，则获取下一页
    auto next_page_id = page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID)//若下一页无效，则新建下一页
    {
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page == nullptr || next_page_id == INVALID_PAGE_ID)//若新建失败，则返回false
      {
        buffer_pool_manager_->UnpinPage(page_id, false);
        return false;
      }
      else{//若创建成功
        page_num++;
        new_page->Init(next_page_id, page_id, log_manager_, txn);//并初始化新页
        page->SetNextPageId(next_page_id);//将其设置为上一页的下一页
        buffer_pool_manager_->UnpinPage(page_id, false);//解引用
        page = new_page;//更新page
        page_id = next_page_id;//更新page_id
      }
    }
    else{//若下一页有效，获得下一页并作为当前页，继续循环
      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = next_page_id;
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
  }
  return false;
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
 * Done
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if(page == nullptr)
  {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
  Row old_row;//定义一个row
  old_row.SetRowId(rid);//设置rowid
  //这里将tablepage的updatetuple函数进行了修改，返回值分为1,-1，-2，-3
  int res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(res == 1)//返回1说明一切正常
  {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  else if(res == -3)//返回-3，则表明剩余的空闲空间加上旧元组的大小小于新元组的序列化大小
  {
    ApplyDelete(rid, txn);//可先删除旧元组
    InsertTuple(row, txn);//再将新元组进行插入
    buffer_pool_manager_->UnpinPage(page_id, true);//脏页
    //Log(INFO) << "Table_Heap::UpdateTuple() succeed: " << "page_id: " << page_id;
    return true;
  }
  //剩下情况都直接返回false
  return false;
}

/**
 * Done
 * 1. Find the page which contains the tuple.
 * 2. Delete the tuple from the page.
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if(page == nullptr)//如果此页不存在，则什么都不做
  {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return;
  }
  else {//否则，删除该行，并标记位脏页
    page->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(page_id, true);
  }
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
 * Done
 *
 * 1. row->rowid->pageid->page
 * 2. 如果没有获取到 page，返回 false
 * 3. 调用 page->GetTuple,读取键值对。
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  RowId rowid = row->GetRowId();
  auto page_id = rowid.GetPageId();//找到row所在的page
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if(page == nullptr){
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  if(page->GetTuple(row, schema_, txn, lock_manager_))
  {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);
  return false;
}

/**
 * 1. 如果page_id 为 INVALID_PAGE_ID，删除第一个页
 * 2. 如果不是，fetchpage 并删除
 */
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
 * Done
 * 1. 从第一页开始，如果 page_id 为 INVALID_PAGE_ID，返回 end
 * 2. 否则，找到第一个有效的页，并返回迭代器
 *
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t page_id = first_page_id_;//取出首页id
  RowId result_rid;
  while(1)
  {
    if(page_id == INVALID_PAGE_ID)//如果当前页不可用，直接返回end
    {
      return End();
    }
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(page->GetFirstTupleRid(&result_rid))//获取第一个元组id
    {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;//获取成功则退出循环
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = page->GetNextPageId();//如果获取失败，说明该页已经无效（通过观察GetFirstTupleRid得出），则寻找下一页
  }
  if(page_id != INVALID_PAGE_ID)//获取元组成功
  {
    Row* result_row = new Row(result_rid);//用找到的元组id构造row
    GetTuple(result_row, txn);//用该row获取tuple
    return TableIterator(this, result_rid, txn, result_row);//返回迭代器
  }
  return End();
}

/**
 * Done
 */
TableIterator TableHeap::End() {
  RowId null;//创建的是一个无效的行ID，并不对应于堆表中的任何行
  return TableIterator(this, null, nullptr, nullptr);
}
