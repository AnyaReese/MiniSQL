#include "page/table_page.h"

// TODO: Update interface implementation if apply recovery

// 初始化页面，包括设置页面ID、前一个页面ID、下一个页面ID、空闲空间指针和元组数量。
void TablePage::Init(page_id_t page_id, page_id_t prev_id, LogManager *log_mgr, Txn *txn) {
  memcpy(GetData(), &page_id, sizeof(page_id));
  SetPrevPageId(prev_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetFreeSpacePointer(PAGE_SIZE);
  SetTupleCount(0);
}


// 插入一个新的元组。如果有可重用的空闲槽位则复用，否则使用空闲空间插入新的元组，并更新相关的元组偏移和大小信息。
bool TablePage::InsertTuple(Row &row, Schema *schema, Txn *txn, LockManager *lock_manager, LogManager *log_manager) {
  uint32_t serialized_size = row.GetSerializedSize(schema);
  ASSERT(serialized_size > 0, "Can not have empty row.");
  if (GetFreeSpaceRemaining() < serialized_size + SIZE_TUPLE) {
    return false;
  }
  // Try to find a free slot to reuse.
  uint32_t i;
  for (i = 0; i < GetTupleCount(); i++) {
    // If the slot is empty, i.e. its tuple has size 0,
    if (GetTupleSize(i) == 0) {
      // Then we break out of the loop at index i.
      break;
    }
  }
  if (i == GetTupleCount() && GetFreeSpaceRemaining() < serialized_size + SIZE_TUPLE) {
    return false;
  }
  // Otherwise we claim available free space..
  SetFreeSpacePointer(GetFreeSpacePointer() - serialized_size);
  uint32_t __attribute__((unused)) write_bytes = row.SerializeTo(GetData() + GetFreeSpacePointer(), schema);
  ASSERT(write_bytes == serialized_size, "Unexpected behavior in row serialize.");

  // Set the tuple.
  SetTupleOffsetAtSlot(i, GetFreeSpacePointer());
  SetTupleSize(i, serialized_size);
  // Set rid
  row.SetRowId(RowId(GetTablePageId(), i));
  if (i == GetTupleCount()) {
    SetTupleCount(GetTupleCount() + 1);
  }
  return true;
}

// 标记元组为已删除。通过设置元组大小的删除标志位来标记删除。
bool TablePage::MarkDelete(const RowId &rid, Txn *txn, LockManager *lock_manager, LogManager *log_manager) {
  uint32_t slot_num = rid.GetSlotNum();
  // If the slot number is invalid, abort.
  if (slot_num >= GetTupleCount()) {
    return false;
  }
  uint32_t tuple_size = GetTupleSize(slot_num);
  // If the tuple is already deleted, abort.
  if (IsDeleted(tuple_size)) {
    return false;
  }
  // Mark the tuple as deleted.
  if (tuple_size > 0) {
    SetTupleSize(slot_num, SetDeletedFlag(tuple_size));
  }
  return true;
}

// 更新现有元组。如果有足够的空闲空间直接更新，否则需要通过删除后重新插入来完成更新操作。更新时维护所有元组的偏移。
bool TablePage::UpdateTuple(Row &new_row, Row *old_row, Schema *schema, Txn *txn, LockManager *lock_manager,
                            LogManager *log_manager) {
  ASSERT(old_row != nullptr && old_row->GetRowId().Get() != INVALID_ROWID.Get(), "invalid old row.");
  uint32_t serialized_size = new_row.GetSerializedSize(schema);
  ASSERT(serialized_size > 0, "Can not have empty row.");
  uint32_t slot_num = old_row->GetRowId().GetSlotNum();
  // If the slot number is invalid, abort.
  if (slot_num >= GetTupleCount()) {
    return false;
  }
  uint32_t tuple_size = GetTupleSize(slot_num);
  // If the tuple is deleted, abort.
  if (IsDeleted(tuple_size)) {
    return false;
  }
  // If there is not enough space to update, we need to update via delete followed by an insert (not enough space).
  if (GetFreeSpaceRemaining() + tuple_size < serialized_size) {
    return false;
  }
  // Copy out the old value.
  uint32_t tuple_offset = GetTupleOffsetAtSlot(slot_num);
  uint32_t __attribute__((unused)) read_bytes = old_row->DeserializeFrom(GetData() + tuple_offset, schema);
  ASSERT(tuple_size == read_bytes, "Unexpected behavior in tuple deserialize.");
  uint32_t free_space_pointer = GetFreeSpacePointer();
  ASSERT(tuple_offset >= free_space_pointer, "Offset should appear after current free space position.");
  memmove(GetData() + free_space_pointer + tuple_size - serialized_size, GetData() + free_space_pointer,
          tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size - serialized_size);
  new_row.SerializeTo(GetData() + tuple_offset + tuple_size - serialized_size, schema);
  SetTupleSize(slot_num, serialized_size);

  // Update all tuple offsets.
  for (uint32_t i = 0; i < GetTupleCount(); ++i) {
    uint32_t tuple_offset_i = GetTupleOffsetAtSlot(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset + tuple_size) {
      SetTupleOffsetAtSlot(i, tuple_offset_i + tuple_size - new_row.GetSerializedSize(schema));
    }
  }
  return true;
}

// 实际删除元组，包括调整空闲空间指针和更新元组偏移。用于在事务提交时实际删除元组。
void TablePage::ApplyDelete(const RowId &rid, Txn *txn, LogManager *log_manager) {
  uint32_t slot_num = rid.GetSlotNum();
  ASSERT(slot_num < GetTupleCount(), "Cannot have more slots than tuples.");

  uint32_t tuple_offset = GetTupleOffsetAtSlot(slot_num);
  uint32_t tuple_size = GetTupleSize(slot_num);
  // Check if this is a delete operation, i.e. commit a delete.
  if (IsDeleted(tuple_size)) {
    tuple_size = UnsetDeletedFlag(tuple_size);
  }

  uint32_t free_space_pointer = GetFreeSpacePointer();
  ASSERT(tuple_offset >= free_space_pointer, "Free space appears before tuples.");

  memmove(GetData() + free_space_pointer + tuple_size, GetData() + free_space_pointer,
          tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size);
  SetTupleSize(slot_num, 0);
  SetTupleOffsetAtSlot(slot_num, 0);

  // Update all tuple offsets.
  for (uint32_t i = 0; i < GetTupleCount(); ++i) {
    uint32_t tuple_offset_i = GetTupleOffsetAtSlot(i);
    if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset) {
      SetTupleOffsetAtSlot(i, tuple_offset_i + tuple_size);
    }
  }
}

// 回滚删除操作，解除元组的删除标记。用于在事务回滚时恢复元组。
void TablePage::RollbackDelete(const RowId &rid, Txn *txn, LogManager *log_manager) {
  uint32_t slot_num = rid.GetSlotNum();
  ASSERT(slot_num < GetTupleCount(), "We can't have more slots than tuples.");
  uint32_t tuple_size = GetTupleSize(slot_num);

  // Unset the deleted flag.
  if (IsDeleted(tuple_size)) {
    SetTupleSize(slot_num, UnsetDeletedFlag(tuple_size));
  }
}

// 获取元组的数据，将其反序列化到提供的 Row 对象中。检查元组是否存在以及是否被删除。
bool TablePage::GetTuple(Row *row, Schema *schema, Txn *txn, LockManager *lock_manager) {
  ASSERT(row != nullptr && row->GetRowId().Get() != INVALID_ROWID.Get(), "Invalid row.");
  // Get the current slot number.
  uint32_t slot_num = row->GetRowId().GetSlotNum();
  // If somehow we have more slots than tuples, abort the recovery.
    if (slot_num >= GetTupleCount()) {
    return false;
  }
  // Otherwise get the current tuple size too.
  uint32_t tuple_size = GetTupleSize(slot_num);
  // If the tuple is deleted, abort the recovery.
  if (IsDeleted(tuple_size)) {
    return false;
  }
  // At this point, we have at least a shared lock on the RID. Copy the tuple data into our result.
  uint32_t tuple_offset = GetTupleOffsetAtSlot(slot_num);
  uint32_t __attribute__((unused)) read_bytes = row->DeserializeFrom(GetData() + tuple_offset, schema);
  ASSERT(tuple_size == read_bytes, "Unexpected behavior in tuple deserialize.");
  return true;
}

// 查找并返回第一个有效（未被删除）的元组的 RowId。
bool TablePage::GetFirstTupleRid(RowId *first_rid) {
  // Find and return the first valid tuple.
  for (uint32_t i = 0; i < GetTupleCount(); i++) {
    if (!IsDeleted(GetTupleSize(i))) {
      first_rid->Set(GetTablePageId(), i);
      return true;
    }
  }
  first_rid->Set(INVALID_PAGE_ID, 0);
  return false;
}

// 查找并返回当前 RowId 之后的第一个有效（未被删除）的元组的 RowId。用于遍历元组。
bool TablePage::GetNextTupleRid(const RowId &cur_rid, RowId *next_rid) {
  ASSERT(cur_rid.GetPageId() == GetTablePageId(), "Wrong table!");
  // Find and return the first valid tuple after our current slot number.
  for (auto i = cur_rid.GetSlotNum() + 1; i < GetTupleCount(); i++) {
    if (!IsDeleted(GetTupleSize(i))) {
      next_rid->Set(GetTablePageId(), i);
      return true;
    }
  }
  // Otherwise return false as there are no more tuples.
  next_rid->Set(INVALID_PAGE_ID, 0);
  return false;
}
