#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* DONE
*/
uint32_t Column::SerializeTo(char *buf) const {
  char *p = buf;

  // magic_num
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += sizeof(uint32_t);

  // column_name_length
  MACH_WRITE_UINT32(buf, name_.length());
  buf += sizeof(uint32_t);

  // column_name
  MACH_WRITE_STRING(buf, name_);
  buf += name_.length();

  // type
  MACH_WRITE_UINT32(buf, static_cast<uint32_t>(type_));
  buf += sizeof(uint32_t);

  // len
  MACH_WRITE_UINT32(buf, len_);
  buf += sizeof(uint32_t);

  // table ind
  MACH_WRITE_UINT32(buf, table_ind_);
  buf += sizeof(uint32_t);

  // nullable
  MACH_WRITE_UINT32(buf, nullable_);
  buf += sizeof(uint32_t);

  // unique
  MACH_WRITE_UINT32(buf, unique_);
  buf += sizeof(uint32_t);

  return buf - p;
}

/**
 * DONE
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) * 7 + name_.length();
}

/**
 * DONE
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  /* deserialize field from buf */

  void *mem = malloc(sizeof(Column));
  char *p = buf;

  // COLUMN_MAGIC_NUM
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Failed to deserialize Column.");

  // name_.length
  uint32_t name_length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // name_
  std::string name = MACH_READ_STRING(buf, name_length);
  buf += name_length;

  // type_
  TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buf));
  buf += sizeof(uint32_t);

  // len_
  uint32_t len = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // table_ind_
  uint32_t table_ind = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // nullable_
  bool nullable = MACH_READ_UINT32(buf) != 0;
  buf += sizeof(uint32_t);

  // unique_
  bool unique = MACH_READ_UINT32(buf) != 0;
  buf += sizeof(uint32_t);

  if (type == TypeId::kTypeChar) {
    column = new (mem) Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new (mem) Column(name, type, table_ind, nullable, unique);
  }

  return buf - p;
}
