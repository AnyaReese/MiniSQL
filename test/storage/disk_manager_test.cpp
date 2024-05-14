#include "storage/disk_manager.h"
#include "glog/logging.h"
#include <unordered_set>

#include "gtest/gtest.h"

TEST(DiskManagerTest, BitMapPageTest) {
  const size_t size = 512;
  char buf[size];
  
  memset(buf, 0, size);
  BitmapPage<size> *bitmap = reinterpret_cast<BitmapPage<size> *>(buf);
  auto num_pages = bitmap->GetMaxSupportedSize();
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->IsPageFree(i));
  }
  
  uint32_t ofs;
  std::unordered_set<uint32_t> page_set;
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->AllocatePage(ofs));
    ASSERT_TRUE(page_set.find(ofs) == page_set.end());
    page_set.insert(ofs);
  }

  
  ASSERT_FALSE(bitmap->AllocatePage(ofs));
  ASSERT_TRUE(bitmap->DeAllocatePage(233));
  ASSERT_TRUE(bitmap->AllocatePage(ofs));
  ASSERT_EQ(233, ofs);
  for (auto v : page_set) {
    ASSERT_TRUE(bitmap->DeAllocatePage(v));
    ASSERT_FALSE(bitmap->DeAllocatePage(v));
  }
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->AllocatePage(ofs));
  }
  ASSERT_FALSE(bitmap->AllocatePage(ofs));
  LOG(WARNING) << "TEST(DiskManagerTest, BitMapPageTest) success!" << std::endl;
}

TEST(DiskManagerTest, FreePageAllocationTest) {
  std::string db_name = "disk_test.db";
  DiskManager *disk_mgr = new DiskManager(db_name);
  int extent_nums = 2;
  // LOG(WARNING) << "test00" << std::endl;
  for (uint32_t i = 0; i < DiskManager::BITMAP_SIZE * extent_nums; i++) {
    // LOG(WARNING) << "test0" << std::endl;
    page_id_t page_id = disk_mgr->AllocatePage();
    // LOG(WARNING) << "test1" << std::endl;
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(i, page_id);
    // LOG(WARNING) << "test2" << std::endl;
    EXPECT_EQ(i / DiskManager::BITMAP_SIZE + 1, meta_page->GetExtentNums());
    EXPECT_EQ(i + 1, meta_page->GetAllocatedPages());
    EXPECT_EQ(i % DiskManager::BITMAP_SIZE + 1, meta_page->GetExtentUsedPage(i / DiskManager::BITMAP_SIZE));
  }
  // LOG(WARNING) << "test01" << std::endl;
  disk_mgr->DeAllocatePage(0);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE - 1);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE + 1);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE + 2);
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
  EXPECT_EQ(extent_nums * DiskManager::BITMAP_SIZE - 5, meta_page->GetAllocatedPages());
  EXPECT_EQ(DiskManager::BITMAP_SIZE - 2, meta_page->GetExtentUsedPage(0));
  EXPECT_EQ(DiskManager::BITMAP_SIZE - 3, meta_page->GetExtentUsedPage(1));
  remove(db_name.c_str());
  LOG(WARNING) << "TEST(DiskManagerTest, FreePageAllocationTest) success!" << std::endl;
}