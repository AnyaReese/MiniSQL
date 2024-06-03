#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}


/**
 * TODO: Student Implement
 */
/**
 * Fetches a page from the buffer pool.
 * 
 * @param page_id The ID of the page to fetch.
 * @return A pointer to the fetched page, or nullptr if the page does not exist or all pages in the buffer pool are pinned.
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  unique_lock<recursive_mutex> lock(latch_);
  if(page_id == INVALID_PAGE_ID)
    return nullptr;

  // 1. Search the page table for the requested page (P).
  frame_id_t frame_id;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 1.1 If P exists, pin it and return it immediately.
    frame_id = it->second;
    // pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }


  // 1.2 If P does not exist, find a replacement page (R) from either the free list or the replacer.
  Page *cache_page = nullptr;
    
  if (!free_list_.empty()) {  // 如果有页面在空闲列表中
    frame_id = free_list_.front();
    cache_page = &pages_[frame_id];
    free_list_.pop_front();
  } else {
    if (replacer_->Victim(&frame_id)) { // 如果空闲列表为空但 replacer_ 有页面可以替换
        cache_page = &pages_[frame_id];  
        // 2. If R is dirty, write it back to the disk.
        if (cache_page->IsDirty()) {
          disk_manager_->WritePage(cache_page->page_id_, cache_page->data_);
        }
        page_table_.erase(cache_page->page_id_);
    }
    else return nullptr;
  }
  // 3. Delete R from the page table and insert P.
  page_table_.emplace(page_id, frame_id);
  // 4. Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  if (pages_[frame_id].pin_count_++ == 0) {
      replacer_->Pin(frame_id);
  }
  return &pages_[frame_id];
}
/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  unique_lock<recursive_mutex> lock(latch_);

  // 0. Make sure you call AllocatePage!

  // Pick a victim page P from either the free list or the replacer.
  frame_id_t frame_id;
  Page *victim_page = nullptr;
  if (!free_list_.empty()) {  // 如果有页面在空闲列表中
    frame_id = free_list_.front();
    free_list_.pop_front();
    victim_page = &pages_[frame_id];
  } 
  else {
    if (replacer_->Victim(&frame_id)) {  // 如果空闲列表为空但 replacer_ 有页面可以替换
      
      victim_page = &pages_[frame_id];
      page_table_.erase(victim_page->page_id_);
    }
    else return nullptr;  // If all the pages in the buffer pool are pinned, return nullptr.
  }
  // LOG(WARNING) << "victim_page: "<<&pages_[frame_id];
  // 3. Update P's metadata, zero out memory and add P to the page table.
  page_id_t new_page_id = AllocatePage();
  // LOG(WARNING) << "test111" << std::endl;
  // Page &page = pages_[frame_id];
  
  // if (victim_page->is_dirty_) {
  //   disk_manager_->WritePage(victim_page->page_id_, victim_page->data_);
  // }

  page_table_.erase(victim_page->page_id_);
  page_table_.emplace(new_page_id, frame_id); // add P to the page table

  // Update P's metadata
  page_id = new_page_id;
  victim_page->page_id_ = new_page_id;
  victim_page->is_dirty_ = false;
  victim_page->pin_count_ = 1;
  memset(victim_page->data_, 0, PAGE_SIZE);
  replacer_->Pin(frame_id);

  // 4. Set the page ID output parameter. Return a pointer to P.
  return &pages_[frame_id];
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  unique_lock<recursive_mutex> lock(latch_);

  // 0. Make sure you call DeallocatePage!


  // 1. Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) 
    return true;
  
  // 2. If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pages_[it->second].pin_count_ > 0) 
    return false;


  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];
  // 3. Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page.IsDirty()) {
    FlushPage(page_id);
  }
  page_table_.erase(page_id);
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  unique_lock<recursive_mutex> lock(latch_);

  // 1. Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];

  // 2. If P exists, decrement the pin count. If the pin count is 0, unpin the page.
  if (page.pin_count_ > 0) {
    page.pin_count_--;
    // 3. If the page is dirty, update the page's is_dirty flag.
    if (is_dirty) {
      page.is_dirty_ = true;
    }
    if (page.pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
  }

  return true;

}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  unique_lock<recursive_mutex> lock(latch_);

  // LOG(WARNING) << "CheckAllUnpinned: " << CheckAllUnpinned();
  // 1. Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  // 2. If P exists, write its contents to disk.
  
  LOG(WARNING) << "page_id: " << page_id;
  disk_manager_->WritePage(page_id, pages_[it->second].data_);
  pages_[it->second].is_dirty_ = false;

  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}