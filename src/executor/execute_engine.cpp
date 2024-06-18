#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
**/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    LOG(WARNING)<<stdir->d_name;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(db_name.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "no database selected" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  pSyntaxNode col_list_node = ast->child_->next_->child_;
  vector<string> primary_key_list;
  vector<string> unique_col;
  vector<Column *> Columns;

  int index = 0;
  for (auto it = col_list_node; it != nullptr; it = it->next_) {  // 遍历操作链表
    switch(it->type_){
      case kNodeColumnDefinition:{ // 如果当前节点的类型是定义相关属性
                                                    // 子节点会有属性名，类型
        bool uniqueFlag = it->val_ != nullptr;    // 非空时是unique
        string col_name = it->child_->val_;       // 属性名
        string col_type = it->child_->next_->val_;// 属性类型
        if(uniqueFlag) unique_col.emplace_back(col_name);
        if(col_type == "int"){  // 根据属性类型决定操作
          Column* column = new Column(col_name, kTypeInt, index++, true, uniqueFlag);
          Columns.emplace_back(column);
        }
        else if(col_type == "char"){
          int len;
          try {
            len = int32_t(stoi(it->child_->next_->child_->val_));
          } catch (const std::invalid_argument& e) {
            LOG(WARNING) << "Meet invalid char length: " << len;
            return DB_FAILED;
          }
          Column* column = new Column(col_name, kTypeChar, len, index++, true, uniqueFlag);
          Columns.emplace_back(column);
        }
        else if(col_type == "float"){
          Column* column = new Column(col_name, kTypeFloat, index++, true, uniqueFlag );
          Columns.emplace_back(column);
        }
        else{
          LOG(WARNING) << "Meet invalid column type: " << col_name;
          return DB_FAILED;
        }
        break;
      }
      case kNodeColumnList:{ // 如果是在定义主键
        for (auto key_it = it->child_; key_it != nullptr; key_it = key_it->next_) {
          primary_key_list.emplace_back(key_it->val_);
        }
        break;
      }
      default:{            // 如果都不是，异常
        LOG(WARNING) << "Meet invalid column type: " << it->type_;
        return DB_FAILED;
        break;
      }
    }
  }

  Schema *schema = new Schema(Columns); // 新建一个Schema
  CatalogManager *catalog_manager = context->GetCatalog();
  Txn* transaction  = context->GetTransaction();
  TableInfo *table_info = nullptr;
  dberr_t CreateTableErr = catalog_manager->CreateTable(table_name, schema, transaction, table_info);
  if (CreateTableErr != DB_SUCCESS) return CreateTableErr;  // 如果建表失败，则返回报错

  for (auto it : unique_col) { // 为所有Unique列创建索引
    string index_name = it + "_UNIQUE" + "_" + "ON_" + table_name;
    IndexInfo* index_info(nullptr);
    catalog_manager->CreateIndex(table_name, index_name, vector<string>{it}, transaction, index_info, "btree");
  }

  if (!primary_key_list.empty()) {    // 如果主键列表非空，表示需要定义主键
    string index_name = table_name + "_PRIMARY_KEY";
    IndexInfo* index_info(nullptr);
    catalog_manager->CreateIndex(table_info->GetTableName(), index_name, primary_key_list, transaction, index_info, "bptree");
  }
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "no database selected" << endl;
    return DB_FAILED;
  }

  CatalogManager *catalog_manager = context->GetCatalog();

  // Drop the table.
  string table_name(ast->child_->val_);
  dberr_t DropTableErr = catalog_manager->DropTable(table_name);
  if (DropTableErr != DB_SUCCESS) return DropTableErr;


  // Gain the index
  std::vector<IndexInfo *> index_info_VEC;
  dberr_t GetTableIndexesErr = catalog_manager->GetTableIndexes(table_name, index_info_VEC);
  if (GetTableIndexesErr != DB_SUCCESS) return GetTableIndexesErr;

  // Drop the index.
  for (auto index_info : index_info_VEC) {
    dberr_t DropIndexErr = catalog_manager->DropIndex(table_name, index_info->GetIndexName());
    if (DropIndexErr != DB_SUCCESS) return DropIndexErr;
  }
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected" << endl;
    return DB_FAILED;
  }

  // Get all tables to get indexes.
  std::vector<TableInfo *> table_info_VEC;
  CatalogManager *catalog_manager = context->GetCatalog();
  dberr_t GetTablesErr = catalog_manager->GetTables(table_info_VEC);
  if(GetTablesErr == DB_FAILED) return GetTablesErr;


  // Use map to store the table name and its index info.
  string table_title("Table");
  string index_title("Index");
  uint max_width_table = table_title.length();
  uint max_width_index = index_title.length();

  for (auto table_info : table_info_VEC) {
    std::vector<IndexInfo *> index_info_VEC;
    string table_name = table_info->GetTableName();
    catalog_manager->GetTableIndexes(table_name, index_info_VEC);
    unsigned int table_length = table_info->GetTableName().length();
    max_width_table = (max_width_table > table_length) ?
                                                       max_width_table : table_length;
    for(auto index_info : index_info_VEC){
      unsigned int info_length = index_info->GetIndexName().length();
      max_width_index = (max_width_index > info_length) ?
                                                        max_width_index : info_length;
    }
  }

  cout << "+" << setfill('-') << setw(max_width_table + 2)  << "" << "+";
  cout << setfill('-') << setw(max_width_index + 2) << "" << "+" << endl;

  cout << "| " << std::left << setfill(' ') << setw(max_width_table) << table_title << " |";
  cout << " " << std::left << setfill(' ') << setw(max_width_index) << index_title << " |" << endl;

  cout << "+" << setfill('-') << setw(max_width_table + 2)  << "" << "+";
  cout << setfill('-') << setw(max_width_index + 2) << "" << "+" << endl;

  for (auto table_info: table_info_VEC) {
    vector<IndexInfo*> index_info_VEC;
    string table_name = table_info->GetTableName();
    catalog_manager->GetTableIndexes(table_name, index_info_VEC);
    for(auto index_info : index_info_VEC) {
      cout << "| " << std::left << setfill(' ') << setw(max_width_table) << table_info->GetTableName() << " |";
      cout << " " << std::left << setfill(' ') << setw(max_width_index) << index_info->GetIndexName() << " |" << endl;
    }
  }
  cout << "+" << setfill('-') << setw(max_width_table + 2)  << "" << "+";
  cout << setfill('-') << setw(max_width_index + 2) << "" << "+" << endl;

  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) { // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  string index_name(ast->child_->val_); // 索引名
  string table_name(ast->child_->next_->val_);  // 列表名
  string index_type("bptree");  // 默认是bptree
  if (ast->child_->next_->next_->next_ != nullptr) {  // 如果有指定的类型，则更新
    index_type = string(ast->child_->next_->next_->next_->child_->val_);
  }

  vector<string> column_names;
  for (auto it = ast->child_->next_->next_->child_; it != nullptr; it = it->next_) {
    column_names.emplace_back(it->val_);
  }

  // Get table.
  TableInfo *table_info;
  dberr_t GetTableErr = catalog_manager->GetTable(table_name, table_info);
  if (GetTableErr != DB_SUCCESS) return GetTableErr;

  // Create index on the table.
  IndexInfo *index_info;
  Txn* transaction = context->GetTransaction();
  dberr_t CreateIndexErr = catalog_manager->CreateIndex(table_name, index_name, column_names, transaction, index_info, index_type);
  if (CreateIndexErr != DB_SUCCESS) return CreateIndexErr;
  return DB_SUCCESS;
  // // Insert old records into the new index.
  // auto row_begin = table_info->GetTableHeap()->Begin(transaction);
  // auto row_end = table_info->GetTableHeap()->End();
  // for (auto row = row_begin; row != row_end; ++row) {
  //   auto rid = (*row).GetRowId();
  //   // Get related fields.
  //   vector<Field> fields;
  //   for (auto col : index_info->GetIndexKeySchema()->GetColumns()) {
  //     fields.push_back(*(*row).GetField(col->GetTableInd()));
  //   }
  //   // The row to be inserted into index.
  //   Row row_idx(fields);
  //   dberr_t InsertEntryErr = index_info->GetIndex()->InsertEntry(row_idx, rid, transaction);
  //   if (InsertEntryErr != DB_SUCCESS) return InsertEntryErr;
  // }
  // cout << "Create index " << index_name << endl;

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) { // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  CatalogManager* catalog_manager = context->GetCatalog();

  string index_name = ast->child_->val_; // 要删除的索引的名字
  bool delete_at_least_once=false;
  vector<TableInfo *> table_info_VEC;
  dberr_t GetTablesErr = catalog_manager->GetTables(table_info_VEC);
  if (GetTablesErr != DB_SUCCESS) return GetTablesErr;

  for (auto table_info : table_info_VEC) {  // 遍历所有表
    IndexInfo *index_info;
    string table_name = table_info->GetTableName();
    dberr_t GetIndexErr = catalog_manager->GetIndex(table_name, index_name, index_info);
    if (GetIndexErr == DB_SUCCESS) {  // 如果找到这个索引
      catalog_manager->DropIndex(table_name, index_name);
      delete_at_least_once = true;
    }
  }
  if (!delete_at_least_once) {
    std::cout << "No related table found." << endl;
    return DB_INDEX_NOT_FOUND;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // 当前文件的目录——cmake_build_debug_wsl>bin
  // 重定向输入流到文件中执行sql语句，在main中：若文件到头了，重定向回终端输入
  file_start_time = std::chrono::system_clock::now(); // 记录文件开始读的时间
  freopen(ast->child_->val_, "r", stdin);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
