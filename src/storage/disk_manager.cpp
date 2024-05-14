#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * DONE
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage* metaPage = reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  uint32_t total_extents = metaPage->GetExtentNums();
  uint32_t num_allocated_pages_ = metaPage->GetAllocatedPages();
  uint32_t extents_id = 0;

  


  // 找到最近一个没有满的分区
  while(metaPage->extent_used_page_[extents_id] == BitmapPage<PAGE_SIZE>::GetMaxSupportedSize() && extents_id < total_extents) extents_id++;
  uint32_t bitmapPageId = 1 + extents_id * (BITMAP_SIZE + 1);
  if (extents_id == total_extents) { // 如果所有分区都满了
      auto new_extent = new BitmapPage<PAGE_SIZE>();
      // LOG(WARNING) << "WritePhysicalPage bitmapPageId_: " << bitmapPageId;
      WritePhysicalPage(bitmapPageId, reinterpret_cast<char *>(new_extent));
      metaPage->extent_used_page_[total_extents] = 0;
      metaPage->num_extents_++;
      total_extents++;
  }
  // LOG(WARNING) << "bitmapPageId: " << bitmapPageId;
  
  
  auto bitmap = new BitmapPage<PAGE_SIZE>();
  
  // 更新元信息页


  ReadPhysicalPage(bitmapPageId, reinterpret_cast<char*>(bitmap));
  
  uint32_t page_offset = 0;
  // LOG(WARNING) << "next_free_page_: " << bitmap->next_free_page_;
  // LOG(WARNING) << "page_allocated_: " << bitmap->page_allocated_;
  

  bitmap->AllocatePage(page_offset);

  WritePhysicalPage(bitmapPageId, reinterpret_cast<char *>(bitmap));
  metaPage->num_allocated_pages_++;
  metaPage->extent_used_page_[extents_id]++;
  
  WritePhysicalPage(META_PAGE_ID, meta_data_);


  // 计算并返回逻辑页号
  return extents_id * BITMAP_SIZE + page_offset; // 逻辑页号不包含位图页
}





/**
 * DONE
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  LOG(WARNING) << "DeAllocatePage: ";
  DiskFileMetaPage* metaPage = reinterpret_cast<DiskFileMetaPage*>(GetMetaData());

  int extents_id = logical_page_id / BITMAP_SIZE; // 计算分区索引
  int page_offset = logical_page_id % BITMAP_SIZE; // 计算分区内的页位置

  int bitmapPageId = 1 + extents_id * (BITMAP_SIZE + 1); // 计算位图页的物理页号

  auto bitmap = new BitmapPage<PAGE_SIZE>();
  ReadPhysicalPage(bitmapPageId, reinterpret_cast<char*>(bitmap));

  if (!bitmap->IsPageFree(page_offset)) {
    bitmap->DeAllocatePage(page_offset);

    WritePhysicalPage(bitmapPageId, reinterpret_cast<const char*>(bitmap));



    // 更新元信息页
    DiskFileMetaPage* metaPage = reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
    metaPage->num_allocated_pages_--;
    metaPage->extent_used_page_[extents_id]--;
    WritePhysicalPage(META_PAGE_ID, meta_data_);
  }
}




/**
 * DONE
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  int extents_id = logical_page_id / BITMAP_SIZE;
  int page_offset = logical_page_id % BITMAP_SIZE - 1; // -1 to account for the bitmap page
  if (page_offset < 0) {
    // The first page in each extent is a bitmap page, not a data page.
    return false;
  }

  int bitmapPageId = 1 + extents_id * (BITMAP_SIZE + 1);

  // Reading the bitmap page from disk
  BitmapPage<PAGE_SIZE> bitmap;
  ReadPhysicalPage(bitmapPageId, reinterpret_cast<char*>(&bitmap));

  // Check if the page is free
  return bitmap.IsPageFree(page_offset);
}

/**
 * DONE
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) { //将逻辑页号转换成物理页号
  uint32_t extent_meta_page_num = (logical_page_id / BITMAP_SIZE + 1);
  uint32_t total_extra_meta_page_num = extent_meta_page_num + 1;
  return total_extra_meta_page_num + logical_page_id;
}


int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    

    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
      LOG(INFO) << "Read less than a page" << std::endl;
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);

  // if(physical_page_id == 1){
  //   const BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<const BitmapPage<PAGE_SIZE> *>(page_data);
  //   LOG(INFO) << "BitmapPage next_free_page_: " << bitmap_page->next_free_page_;
  // }


  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}