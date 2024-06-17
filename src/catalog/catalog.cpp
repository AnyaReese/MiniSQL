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
  uint32_t magic_size = sizeof(CATALOG_METADATA_MAGIC_NUM);                                        // 魔数的大小
  uint32_t table_size = sizeof(decltype(table_meta_pages_.size()));                                // 表的大小
  uint32_t index_size = sizeof(decltype(index_meta_pages_.size()));                                // 索引的大小
  uint32_t table_page_size = table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t));  // 表页的大小
  uint32_t index_page_size = index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));  // 索引页的大小
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
  if (!init) {                                                                 // 如果不是初次创建
    auto catalog_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);  // 获取页面以获取CatalogMeta的信息
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page->GetData()));
    next_index_id_ = catalog_meta_->GetNextIndexId();                     // 赋值私有变量next_index_id_
    next_table_id_ = catalog_meta_->GetNextTableId();                     // 赋值私有变量next_table_id_
    for (auto it : catalog_meta_->table_meta_pages_) {                    // 获取所有表的元信息的目录
      auto table_meta_page = buffer_pool_manager_->FetchPage(it.second);  // 获取该表元信息所在的页
      TableMetadata *table_meta;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);  // 将表的元信息反序列化出来
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();     // 获取表的名字
      // 根据已有信息创建一个堆表
      auto table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                          log_manager_, lock_manager_);
      TableInfo *table_info = TableInfo::Create();  // 创建和初始化table_info，为后面赋值tables_做准备
      table_info->Init(table_meta, table_heap);
      tables_[table_meta->GetTableId()] = table_info;    // 赋值tables_
      if (table_meta->GetTableId() >= next_table_id_) {  // 如果当前tableid比nexttableid大，则更新nexttableid
        next_table_id_ = table_meta->GetTableId() + 1;
      }
    }
    for (auto it : catalog_meta_->index_meta_pages_) {                    // 获取所有索引的元信息的目录
      auto index_meta_page = buffer_pool_manager_->FetchPage(it.second);  // 获取该索引元信息所在的页
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);  // 将索引的元信息反序列化出来
      index_names_[tables_[index_meta->GetTableId()]->GetTableName()][index_meta->GetIndexName()] =
          index_meta->GetIndexId();                 // index_names_[表名][索引名] = index_id
      IndexInfo *index_info = IndexInfo::Create();  // 创建和初始化index_info，为后面赋值indexes_做准备
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_[index_meta->GetIndexId()] = index_info;   // 赋值indexes
      if (index_meta->GetIndexId() >= next_index_id_) {  // 如果当前indexid大于nextindexid，更新nextindexid
        next_index_id_ = index_meta->GetIndexId() + 1;
      }
    }
  } else {  // 如果是初次创建，直接新建一个新的catalog_meta_
    next_table_id_ = next_index_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance();
  }
}

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
  try {
    auto iter = table_names_.find(table_name);
    if (iter != table_names_.end()) {  // 判断这个表是否已经存在
      return DB_TABLE_ALREADY_EXIST;
    };
    // 定义一些临时变量
    page_id_t meta_page_id = 0;
    Page *meta_page = nullptr;
    page_id_t table_page_id = 0;
    Page *table_page = nullptr;
    table_id_t table_id = 0;
    TableMetadata *table_meta_ = nullptr;
    TableHeap *table_heap_ = nullptr;
    TableSchema *schema_ = nullptr;

    table_id = catalog_meta_->GetNextTableId();  // 获取一个table_id
    schema_ = schema_->DeepCopySchema(schema);  // 深拷贝，使得如果schema在函数执行期间被修改，不会影响到正在创建的表
    meta_page = buffer_pool_manager_->NewPage(meta_page_id);                          // 获得一个新的meta_page
    table_page = buffer_pool_manager_->NewPage(table_page_id);                        // 获得一个新的table_page
    table_meta_ = table_meta_->Create(table_id, table_name, table_page_id, schema_);  // 初始化table_meta
    table_meta_->SerializeTo(meta_page->GetData());  // 将table_meta_序列化到meta_page中
    table_heap_ =
        table_heap_->Create(buffer_pool_manager_, schema_, txn, log_manager_, lock_manager_);  // 初始化table_heap
    table_info = table_info->Create();                                                         // 初始化table_info
    table_info->Init(table_meta_, table_heap_);

    // 赋值table_names_和tables_，使能通过table_name找到对应的表的元信息和堆表存储
    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

    // 赋值table_meta_pages_
    catalog_meta_->table_meta_pages_[table_id] = meta_page_id;
    Page *page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char *buf = page_->GetData();
    catalog_meta_->SerializeTo(buf);                              // 将catalog序列化到CATALOG_META_PAGE_ID
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);  // 脏页为true
    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/** DONE
 * @brief 获取表的元数据。
 * @param table_name 要获取的表的名称。
 * @param table_info 返回表的元数据。
 * @return dberr_t 表示操作的状态（成功或具体错误类型）
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.count(table_name) <= 0)  // 如果没找到，返回不存在
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];  // 根据name找到id
  table_info = tables_[table_id];                  // 根据id找到info
  return DB_SUCCESS;
}

/** DONE
 * @brief 获取所有表的元数据。
 * @param tables 返回所有表的元数据。
 * @return dberr_t 表示操作的状态（成功或具体错误类型）
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.size() == 0)  // 如果没有，返回failed
    return DB_FAILED;
  tables.resize(tables_.size());
  uint32_t i = 0;
  for (auto itera = tables_.begin(); itera != tables_.end(); ++itera, ++i)
    tables[i] = itera->second;  // tables_中，值存的是info

  return DB_SUCCESS;
}

/** DONE
 * 新建一个索引。
 * @param index_info 返回新创建的索引的信息。
 * @param index_keys 索引的键。
 * @param index_name
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  try {
    // 先要保证这个表是存在的
    auto iter_find_table = table_names_.find(table_name);
    if (iter_find_table == table_names_.end()) {
      return DB_TABLE_NOT_EXIST;
    }
    // 要保证这个index是不存在的
    auto iter_find_index_table = index_names_.find(table_name);  // 看这个table有没有index
    if (iter_find_index_table != index_names_.end()) {
      auto iter_find_index_name = iter_find_index_table->second.find(index_name);  // 看这个index_name是否已经存在
      if (iter_find_index_name != iter_find_index_table->second.end()) {
        return DB_INDEX_ALREADY_EXIST;
      }
    }

    // 初始化
    table_id_t table_id = 0;
    TableSchema *schema_ = nullptr;
    TableInfo *table_info_ = nullptr;
    // index
    page_id_t meta_page_id = 0;
    Page *meta_page = nullptr;
    index_id_t index_id = 0;
    IndexMetadata *index_meta_ = nullptr;
    // index key map
    std::vector<std::uint32_t> key_map{};
    // 初始化index info
    index_info = index_info->Create();

    // get table schema
    table_id = table_names_[table_name];
    table_info_ = tables_[table_id];
    schema_ = table_info_->GetSchema();
    // create key map
    uint32_t column_index = 0;
    // 利用传递进来的index_keys中的column的名字获取该行的index，并存储到key_map中
    for (auto column_name : index_keys) {
      if (schema_->GetColumnIndex(column_name, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
        return DB_COLUMN_NAME_NOT_EXIST;
      }
      key_map.push_back(column_index);
    }
    // 获取一个新页用来存储索引元信息
    meta_page = buffer_pool_manager_->NewPage(meta_page_id);
    // 获取index id
    index_id = catalog_meta_->GetNextIndexId();
    // 利用四个元素创建索引元信息
    index_meta_ = index_meta_->Create(index_id, index_name, table_id, key_map);
    // 将索引元信息序列化到meta_page中
    index_meta_->SerializeTo(meta_page->GetData());
    // 创建index_info
    index_info->Init(index_meta_, table_info_, buffer_pool_manager_);
    // 存储tablename+indexname -> indexid -> indexinfo
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;
    auto table_heap = table_info_->GetTableHeap();
    vector<Field> f;
    for (auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++) {
      f.clear();
      for (auto pos : key_map) {
        f.push_back(*(it->GetField(pos)));
      }
      Row row(f);
      index_info->GetIndex()->InsertEntry(row, it->GetRowId(), nullptr);
    }

    // 存储meta_page的id
    catalog_meta_->index_meta_pages_[index_id] = meta_page_id;
    Page *page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char *buf = page_->GetData();
    // 将其序列化到page中
    catalog_meta_->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/** DONE
 * 得到索引的信息。
 * @param index_name 索引的名称。
 * @param index_info 返回索引的信息。
 * @return dberr_t 表示操作的状态（成功或具体错误类型）
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // 先确保该table是存在的
  if (index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;

  // 获取<index_name, index_id>的映射,并判断index_name是否存在
  auto idx_name2id = index_names_.find(table_name)->second;
  if (idx_name2id.find(index_name) == idx_name2id.end()) return DB_INDEX_NOT_FOUND;

  // 先获取index_id, 再获取index_info
  index_id_t index_id = idx_name2id[index_name];
  index_info = indexes_.find(index_id)->second;

  return DB_SUCCESS;
}

/**
 * DONE
 * 返回表的所有索引。
 * @param indexes 返回表的所有索引。
 * @param table_name 表的名称。
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // 确保table存在
  auto table_indexes = index_names_.find(table_name);
  if (table_indexes == index_names_.end()) return DB_TABLE_NOT_EXIST;
  // 若table存在，获取对应的<index_name, index_id>映射
  auto indexes_map = table_indexes->second;
  // 将所有的index_info都存储到indexes中
  for (auto it : indexes_map) {
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * DONE
 * Drop Tanle
 * @param table_name Table need to be dropped
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // 先要确保table存在
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // 根据table_name找到table_info
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_to_drop = tables_[table_id];
  if (table_to_drop == nullptr) return DB_FAILED;
  tables_.erase(table_id);         // 清除掉这个id对应的映射
  table_names_.erase(table_name);  // 清除掉这个name对应的映射
  table_to_drop->~TableInfo();     // 析构掉这个table对应的tableinfo
  return DB_SUCCESS;
}

/** DONE
 * Drop index
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  try {
    // 先确定这个索引存在
    auto idx_table2name2id = index_names_.find(table_name);
    if (idx_table2name2id != index_names_.end()) {
      auto idx_name2id = idx_table2name2id->second.find(index_name);  // 根据index_name找到<index_name, index_id>
      if (idx_name2id == idx_table2name2id->second.end()) {           // 确定index_name存在
        return DB_INDEX_NOT_FOUND;
      } else {
        delete indexes_[idx_name2id->second];         // 先把该index_id对应的index_info删掉
        indexes_.erase(idx_name2id->second);          // 再将<index_id, index_info>这个映射关系删掉
        idx_table2name2id->second.erase(index_name);  // 删掉<index_name, index_id>这个映射关系
        return DB_SUCCESS;
      }
    } else {
      return DB_INDEX_NOT_FOUND;
    }
  } catch (exception e) {
    return DB_FAILED;
  }
}

/**
 * DONE
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * DONE
 * This function is used to load table metadata from disk
 * @param table_id
 * @param page_id
 * @return DB_SUCCESS
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  try {
    // init
    Page *meta_page = nullptr;
    page_id_t table_page_id = 0;
    string table_name_ = "";
    TableMetadata *table_meta_ = nullptr;
    TableHeap *table_heap_ = nullptr;
    TableSchema *schema_ = nullptr;
    TableInfo *table_info = nullptr;
    // 先初始化一下table_info
    table_info = table_info->Create();
    // 获取table_meta_page
    meta_page = buffer_pool_manager_->FetchPage(page_id);
    // 将该页中的data反序列化到table_meta中
    table_meta_->DeserializeFrom(meta_page->GetData(), table_meta_);
    // 要确保传进来的table_id和传进来的meta_page中记录的id是一样的
    ASSERT(table_id == table_meta_->GetTableId(), "Load wrong table");
    // 获取table_name, first_page, schema创建table_heap
    table_name_ = table_meta_->GetTableName();
    table_page_id = table_meta_->GetFirstPageId();
    schema_ = table_meta_->GetSchema();
    table_heap_ = table_heap_->Create(buffer_pool_manager_, table_page_id, schema_, nullptr, nullptr);
    // 再用前面反序列化出来的table_meta和table_heap初始化table_info
    table_info->Init(table_meta_, table_heap_);
    // 存储name->id->info的映射
    table_names_[table_name_] = table_id;
    tables_[table_id] = table_info;
    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/**
 * DONE
 * LoadIndex
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  try {
    Page *meta_page = buffer_pool_manager_->FetchPage(page_id);  // 先获取存储索引元信息的页
    IndexMetadata *index_meta = nullptr;
    index_meta->DeserializeFrom(meta_page->GetData(), index_meta);  // 然后将该元信息反序列化到index_meta中

    table_id_t table_id = 0;
    table_id = index_meta->GetTableId();  // 获取表id
    TableInfo *table_info = nullptr;
    table_info = tables_[table_id];  // 利用表id获取table_info
    IndexInfo *index_info = nullptr;
    index_info->Init(index_meta, table_info, buffer_pool_manager_);  // 利用index_meta和table_info创建index_info
    string table_name = "";
    table_name = table_info->GetTableName();  // 获取表名
    string index_name = "";
    index_name = index_meta->GetIndexName();  // 获取索引名字
    // 存储<table_name, index_name> -> index_id -> index_info的映射关系
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;
    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/**
 * DONE
 * get Table
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto it = tables_.find(table_id);
  if (it == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;
  return DB_SUCCESS;
}