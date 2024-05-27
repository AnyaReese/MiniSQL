#include "record/schema.h"

/**
 * DONE
 */


  // static constexpr uint32_t SCHEMA_MAGIC_NUM = 200715;
  // std::vector<Column *> columns_;
  // bool is_manage_ = false; /** if false, don't need to delete pointer to column */


uint32_t Schema::SerializeTo(char *buf) const {
  char *begin = buf;

  uint32_t ofs = GetSerializedSize();
  ASSERT(ofs <= PAGE_SIZE, "Failed to serialize schema.");

  // SCHEMA_MAGIC_NUM
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  buf += sizeof(uint32_t);

  // columns_.size
  MACH_WRITE_UINT32(buf, columns_.size());
  buf += sizeof(uint32_t);
  // columns_
  for (auto column: columns_) {
      buf += column->SerializeTo(buf);
  }

  // is_manage


  return buf - begin;
}
/**
 * DONE
 */
uint32_t Schema::GetSerializedSize() const {
    uint32_t size = 4 + 4;
    for (auto column: columns_) {
        size += column->GetSerializedSize();
    }
    return size;
}
/**
 * DONE
 */
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    char *start = buf;
    // SCHEMA_MAGIC_NUM
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Failed to deserialize schema.");

    // columns_.size
    uint32_t columns_size = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    // columns_
    std::vector<Column *> columns;
    for (uint32_t i = 0; i < columns_size; i++) {
        Column *column = nullptr;
        buf += Column::DeserializeFrom(buf, column);
        columns.emplace_back(column);
    }

    bool is_manage = MACH_READ_UINT32(buf) != 0;
    schema = new Schema(columns, is_manage);
    return buf - start;
}
