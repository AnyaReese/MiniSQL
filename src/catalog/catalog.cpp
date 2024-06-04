#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/** DONE
 * Get the size of the serialized catalog metadata
 * @return the size of the serialized catalog metadata
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //  ASSERT(false, "Not Implemented yet");
  uint32_t magic_size = sizeof(CATALOG_METADATA_MAGIC_NUM);
  uint32_t table_size = sizeof(decltype(table_meta_pages_.size()));
  uint32_t index_size = sizeof(decltype(index_meta_pages_.size()));
  uint32_t table_page_size = table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t));
  uint32_t index_page_size = index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return magic_size + table_size + index_size + table_page_size + index_page_size;
}

CatalogMeta::CatalogMeta() {}

/** DONE
 * Constructor for CatalogManager
 * @param table_id
 * @param page_id
 * @param buffer_pool_manager
 * @param lock_manager
 * @param log_manager
 * @param init
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {

  if (init) {
    next_index_id_ = 0;
    next_table_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance();
  } else {
    auto catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page->GetData()));
    buffer_pool_manager_->UnpinPage(catalog_page->GetPageId(), true);
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();

    /* load table metadata */
    for (auto iter : catalog_meta_->index_meta_pages_) {
      auto table_id = iter.first;
      auto table_page_id = iter.second;
      auto table_meta_page = buffer_pool_manager_->FetchPage(table_page_id);
      TableMetadata *table_meta;
      TableMetadata::DeserializeFrom(reinterpret_cast<char *>(table_meta_page->GetData()), table_meta);
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
      auto table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                          log_manager_, lock_manager_);
      TableInfo *table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);
      tables_[table_meta->GetTableId()] = table_info;
      if (table_meta->GetTableId() >= next_table_id_) {
        next_table_id_ = table_meta->GetTableId() + 1;
      }
    } /* end of loop table metadata */

    /* load index metadata */
    for (auto iter : catalog_meta_->index_meta_pages_) {
      auto index_id = iter.first;
      auto index_page_id = iter.second;
      auto index_meta_page = buffer_pool_manager_->FetchPage(index_page_id);
      IndexMetadata *index_meta;
      IndexMetadata::DeserializeFrom(reinterpret_cast<char *>(index_meta_page->GetData()), index_meta);
      auto table_info = tables_[index_meta->GetTableId()];
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_[index_meta->GetIndexId()] = index_info;
      if (index_meta->GetIndexId() >= next_index_id_) {
        next_index_id_ = index_meta->GetIndexId() + 1;
      }
    } /* end of loop index metadata */
  }
}

/**
 * Destructor for CatalogManager
 */
CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/** DONE
 * @brief 创建新表。
 *
 * @param table_name 要创建的表的名称。
 * @param schema 表的结构描述。
 * @param txn 当前事务。
 * @param table_info 返回新创建的表的信息。
 * @return dberr_t 表示操作的状态（成功或具体错误类型）
 *
 * @details 该方法首先检查表是否已经存在。如果不存在，则创建新的表元数据和数据页，并初始化表元数据和表堆。
 * 序列化表元数据到元数据页，并将新表的信息注册到内部映射中。最后，更新目录元数据。
 */

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  try {
    auto iter = table_names_.find(table_name);

    /* Table already exists */
    if (iter != table_names_.end()) {
      return DB_TABLE_ALREADY_EXIST;
    }

    /* Initialize the params */
    page_id_t meta_page_id = 0;
    Page* meta_page = buffer_pool_manager_->NewPage(meta_page_id);
    page_id_t table_page_id = 0;
    Page* table_page = buffer_pool_manager_->NewPage(table_page_id);
    table_id_t table_id = catalog_meta_->GetNextTableId();
    TableMetadata* table_meta = TableMetadata::Create(table_id, table_name, table_page_id, schema);
    TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
    TableSchema* table_schema = nullptr;

    /* Create table metadata */
    table_schema = table_schema->DeepCopySchema(schema); // ?
    table_meta->SerializeTo(reinterpret_cast<char*>(meta_page->GetData()));

    table_info = TableInfo::Create();
    table_info->Init(table_meta, table_heap);

    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

    catalog_meta_->table_meta_pages_.emplace(table_id, meta_page_id);
    Page* page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(reinterpret_cast<char *>(page_->GetData()));
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * This function is used to load table metadata from disk
 * @param table_id
 * @param page_id
 * @return DB_SUCCESS
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}