#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  //  LOG(INFO) << "BPlusTree() Constructor called leaf_max_size_ = " << leaf_max_size_ << " internal_max_size_ = " << internal_max_size_ << std::endl; 
  auto index_root_page = reinterpret_cast<IndexRootsPage *>
      (buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData()); 
  if(!index_root_page->GetRootId(index_id, &root_page_id_)) {
    root_page_id_ = INVALID_PAGE_ID; 
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true); 
  buffer_pool_manager_->UnpinPage(root_page_id_, true); 
}
/*
 * If current_page_id = INVALID_PAGE_ID, then
 * destroy from the root page, otherwise
 * destroy from the current page
 */
void BPlusTree::Destroy(page_id_t current_page_id) {
  //  LOG(INFO) << "Destroy page! " << current_page_id; 
  if(IsEmpty()) return; 
  if(current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_; 
    root_page_id_ = INVALID_PAGE_ID; 
    UpdateRootPageId(2); 
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData()); 
  if(!page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(page); 
    for(int i = page->GetSize() - 1;  i >= 0;  --i) {
      Destroy(inner->ValueAt(i)); 
    }
  }
  buffer_pool_manager_->DeletePage(page->GetPageId()); 
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false); 
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID) return true; 
  return false; 
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 * 
 * 1. 如果树为空，那么返回 false
 * 2. 调用 FindLeafPage() 找到叶子节点
 * 3. 在叶子节点中查找 key
 * 4. 返回结果
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  // if (IsEmpty()) {
  //   return false; 
  // }

  // LeafPage *leaf_page = FindLeafPage(key, root_page_id_, true); 
  // if (leaf_page == nullptr) {
  //   return false; 
  // }

  // int index = leaf_page->KeyIndex(key); 
  // if (index == -1) {
  //   return false; 
  // }

  // result.push_back(leaf_page->ValueAt(index)); 

  // return true; 

  if(IsEmpty()) return false; 
  auto *leaf_page = FindLeafPage(key, root_page_id_, false); 
  if(leaf_page == nullptr) {
    return false; 
  }
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData()); 
  RowId val; 
  bool Find = leaf->Lookup(key, val, processor_); 
  if(Find) {
    result.push_back(val); 
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false); 
  return Find; 
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 * 
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if(IsEmpty()) {
    StartNewTree(key, value); 
    return true; 
  } else {
    return InsertIntoLeaf(key, value, transaction); 
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 * 
 * 1. 申请一个新的 page
 * 2. 初始化叶子节点
 * 3. 将 key 插入到叶子节点中
 * 4. 更新根节点的 page_id
 * 5. 返回
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto * page = buffer_pool_manager_->NewPage(root_page_id_); 
  if(page == nullptr) {
       LOG(ERROR) << "out of memory" << std::endl; 
  }
  auto * leaf = reinterpret_cast<LeafPage *>(page->GetData()); 
  internal_max_size_ =  leaf_max_size_; 
  leaf_max_size_ = 4064/(processor_.GetKeySize() + sizeof(value))-1; 
  if(internal_max_size_ < 2) {
    internal_max_size_ = 2, leaf_max_size_ = 2; 
  }
  leaf->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_); 
  leaf->Insert(key, value, processor_); 
  buffer_pool_manager_->UnpinPage(root_page_id_, true); 
  UpdateRootPageId(1); 
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 * 
 * 1. 调用 FindLeafPage() 找到叶节点，并在叶节点中查找 key
 * 2. 如果 key 已经存在，那么返回 false
 * 3. 如果 key 不存在，那么插入 key
 * 4. 如果叶子节点的 size 超过了 max_size
 *  4.1 将叶子节点分裂
 *  4.2 更新链表并将新的叶子节点插入到父节点中
 * 5. 返回 true
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  RowId _value; 
  LeafPage * leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID,false)->GetData()); 

  if(leaf_page->Lookup(key, _value, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false); 
    return false;  //if user try to insert duplicate keys return false, otherwise return true.
  } else {
    leaf_page->Insert(key, value, processor_); 
    if(leaf_page->GetSize() >= leaf_page->GetMaxSize()) {
      auto *new_page = Split(leaf_page, transaction); 
      leaf_page->SetNextPageId(new_page->GetPageId()); 
      new_page->SetNextPageId(leaf_page->GetNextPageId()); 
      InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction); 
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true); 
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true); 
    return true; 
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 * 
 * 1. 申请一个新的page
 * 2. 将原来页面的一半数据移动到新的 page 中
 * 3. 返回新的page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id; 
  auto *page = buffer_pool_manager_->NewPage(new_page_id); 
  if(page == nullptr) {
       LOG(ERROR) << "BPlusTree::Split  Out of memory" << std::endl; 
  }
  BPlusTreeInternalPage *new_page = reinterpret_cast<BPlusTreeInternalPage *>(page); 
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize()); 
  node->MoveHalfTo(new_page, buffer_pool_manager_); 
  return new_page; 
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id; 
  auto *page = buffer_pool_manager_->NewPage(new_page_id); 

  BPlusTreeLeafPage *new_page = reinterpret_cast<BPlusTreeLeafPage *>(page); 
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(),node->GetMaxSize()); 
  node->MoveHalfTo(new_page); 
  return new_page; 
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 * 
 * 1. 如果 old_node 是根节点，那么新建一个根节点，将 old_node 和 new_node 插入到新的根节点中
 * 2. 如果 old_node 不是根节点，那么找到 old_node 的父节点，将 old_node 和 new_node 插入到父节点中
 * 3. 如果父节点的 size 超过了 max_size，那么将父节点分裂，然后递归调用 InsertIntoParent()
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Txn *transaction) {
  if(old_node->IsRootPage()) {
    auto *page = buffer_pool_manager_->NewPage(root_page_id_); 
    if(page == nullptr) {
      LOG(ERROR) << "BPlusTree::InsertIntoParen:Out of memory." << std::endl; 
    }
    auto * new_root_page = reinterpret_cast<InternalPage *>(page->GetData()); 
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_); 
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId()); 
    old_node->SetParentPageId(root_page_id_); 
    new_node->SetParentPageId(root_page_id_); 
    buffer_pool_manager_->UnpinPage(root_page_id_, true); 
    UpdateRootPageId(0); 
  } else {
    auto *fa_page = reinterpret_cast<BPlusTree::InternalPage *>(
        buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData()); 
    fa_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()); 
    if (fa_page->GetSize() >= fa_page->GetMaxSize()) {
      InternalPage *fa_split_page = Split(fa_page, transaction); 
      InsertIntoParent(fa_page, fa_split_page->KeyAt(0), fa_split_page, transaction); 
      buffer_pool_manager_->UnpinPage(fa_split_page->GetPageId(), true); 
    }
    buffer_pool_manager_->UnpinPage(fa_page->GetPageId(), true); 
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()) return; 
  auto * leaf = reinterpret_cast<LeafPage *>
      (FindLeafPage(key, INVALID_PAGE_ID,false)->GetData()); 
  int pre_size = leaf->GetSize(); 
  if(pre_size > leaf->RemoveAndDeleteRecord(key, processor_)) {
    CoalesceOrRedistribute(leaf, transaction); 
    // DeletePage ?
  } else {
    //    LOG(ERROR) << "Remove() : RemoveAndDeleteRecord() failed"; 
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false); 
    return; 
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true); 
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  //  LOG(INFO) << "CoalesceOrRedistribute() called"; 
  bool _delete = false; 
  if(node->IsRootPage()) {
    _delete = AdjustRoot(node); 
  } else if (node->GetSize() >= node->GetMinSize()){
    return false; 
  } else {
    page_id_t parent_id = node->GetParentPageId(); 
    auto * par = reinterpret_cast<InternalPage *>(
        buffer_pool_manager_->FetchPage(parent_id) -> GetData()); 
    int index = par->ValueIndex(node->GetPageId()); 
    int sib_index = index == 0 ? 1 : index - 1; 
    page_id_t sibling_id = par->ValueAt(sib_index); 
    auto * sibling = reinterpret_cast<N *>
        (buffer_pool_manager_->FetchPage(sibling_id)->GetData()); 
    if(node->GetSize() + sibling->GetSize() >= node->GetMaxSize()) {
      Redistribute(sibling, node, index); 
      buffer_pool_manager_->UnpinPage(par->GetPageId(), true); 
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true); 
    } else {
      Coalesce(sibling, node, par, index); 
      buffer_pool_manager_->UnpinPage(par->GetPageId(), true); 
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true); 
      _delete = 1; 
    }
  }
  return _delete; 
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int sib_index = index == 0 ? 1 : index - 1; 
  if(index < sib_index) {
    neighbor_node->MoveAllTo(node); 
    node->SetNextPageId(neighbor_node->GetNextPageId()); 
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true); 
    parent->Remove(sib_index);
  } else {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int sib_index = index == 0 ? 1 : index - 1; 
  if(index < sib_index) {
    neighbor_node->MoveAllTo(node, parent->KeyAt(sib_index), buffer_pool_manager_);
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(sib_index);
  } else {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_); 
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(index); 
  }
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto * parent = reinterpret_cast<InternalPage *>
      (buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if(index == 0) {
    neighbor_node->MoveFirstToEndOf(node); 
    parent->SetKeyAt(1, neighbor_node->KeyAt(0)); 
  } else { 
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  } 
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true); 
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto * parent = reinterpret_cast<BPlusTree::InternalPage *>
      (buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if(index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_); 
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node,parent->KeyAt(index), buffer_pool_manager_); 
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true); 
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  //  LOG(INFO) << "AdjustRoot() called";
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID; 
    UpdateRootPageId(0);
    return true;
  } else if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto root = reinterpret_cast<BPlusTree::InternalPage *>(old_root_node);
    auto * only_child = reinterpret_cast<BPlusTreePage *>
        (buffer_pool_manager_->FetchPage(root->ValueAt(0))->GetData());
    only_child->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = only_child->GetPageId(); 
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(only_child->GetPageId(), true); 
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto * page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, INVALID_PAGE_ID, true)->GetData());
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false); 
  return IndexIterator(page_id, buffer_pool_manager_, 0);  
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto * page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false)->GetData());
  int index = page->KeyIndex(key, processor_);
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return IndexIterator(page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  //   auto * page = reinterpret_cast<BPlusTreePage *>
  //       (buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  //   while(!page->IsLeafPage()) {
  //     auto inner = reinterpret_cast<InternalPage *>(page);
  //      page_id_t child_id = inner->ValueAt(inner->GetSize()-1);
  //      auto child_page = reinterpret_cast<BPlusTreePage *>
  //          (buffer_pool_manager_->FetchPage(child_id)->GetData());
  //      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //      page = child_page;
  //   }
  //   int page_id = page->GetPageId(), index = page->GetSize()-1;
  //   buffer_pool_manager_->UnpinPage(page_id, false);
  //   return IndexIterator(page_id, buffer_pool_manager_, index);
  // End() = default;
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 * 
 * 1. 如果 page_id = INVALID_PAGE_ID，那么从根节点开始查找，如果 page_id != INVALID_PAGE_ID，那么从 page_id 开始查找
 * 2. 如果 leftMost = true，那么找到最左边的叶子节点
 * 3. 返回叶子节点
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(page_id == INVALID_PAGE_ID) page_id = root_page_id_;
  auto * page = reinterpret_cast<BPlusTreePage *>
      (buffer_pool_manager_->FetchPage(page_id)->GetData());
  while(!page->IsLeafPage()) {
    InternalPage* internal_page = reinterpret_cast<InternalPage *>(page);
    page_id_t child_id;
    if(leftMost) {
      child_id = internal_page->ValueAt(0);
    } else {
      child_id = internal_page->Lookup(key, processor_);
    }
    BPlusTreePage * child_page = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager_->FetchPage(child_id)->GetData());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child_page;
  }
  return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID, 
 * header_page is defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 * 
 * 1. 从 buffer_pool_manager_ 中获取 header page
 * 2. 如果 insert_record = true，那么插入记录 <index_id_, root_page_id_>，否则更新记录 <index_id_, root_page_id_>
 * 3. 将 header page 放回 buffer_pool_manager_
 * 4. 返回
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto * root = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if(insert_record ==  1) {
    ASSERT(root->Insert(index_id_, root_page_id_), "BPlusTree :: UpdateRootPageId() inserted failed");
  } else if(insert_record ==  0){
    ASSERT(root->Update(index_id_, root_page_id_), "BPlusTree :: UpdateRootPageId() updated failed");
  } else {
    ASSERT(root->Delete(index_id_), "BPlusTree :: UpdateRootPageId() deleted failed");
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_") ;
  std::string internal_prefix("INT_") ;
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page); 
    // Print node name
    out << leaf_prefix << leaf->GetPageId(); 
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}