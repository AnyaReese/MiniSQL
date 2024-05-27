#include "record/row.h"

/**
 * DONE
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  char *p = buf;

  // rid
  RowId rowID = rid_;
  MACH_WRITE_UINT32(buf, rowID.GetPageId());
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, rowID.GetSlotNum());
  buf += sizeof(uint32_t);

  // field
  std::vector<bool> nulls;
  for (auto field: fields_) {
      nulls.push_back(field->IsNull());
  }
  for (auto null: nulls) {
      MACH_WRITE_UINT8(buf, null);
      buf += 1;
  }
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (nulls[i]) {
          continue;
      }
      buf += fields_[i]->SerializeTo(buf);
  }
  return buf - p;
}

/**
 * DONE
 */
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  char *p = buf;

  // rid
  uint32_t page_id = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t slot_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  rid_ = RowId(page_id, slot_num);

  // field
  std::vector<bool> nulls;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      nulls.push_back(MACH_READ_UINT8(buf) != 0);
      buf += 1;
  }
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      Field *field = nullptr;
      buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &field, nulls[i]);
      fields_.emplace_back(field);
  }
  return buf - p;
}

/**
 * DONE
 */
uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t size = 4 + 4 + fields_.size();
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (fields_[i]->IsNull()) {
          continue;
      }
      size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
