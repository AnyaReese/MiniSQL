#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * DONE
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  size_t max_pages = GetMaxSupportedSize();  // 获取位图支持的最大页面数量
  if(page_allocated_==max_pages) return false;

  size_t current_page_offset = next_free_page_;

  if (IsPageFree(current_page_offset)) {
      uint32_t byte_index = current_page_offset / 8;
      uint8_t bit_index = current_page_offset % 8;
      bytes[byte_index] |= (1 << bit_index);  // 将该位设置为1（已分配）
      
      // 更新 next_free_page_ 直到找到下一个空闲页或到达起始点
      next_free_page_ = current_page_offset + 1;
      while (next_free_page_ < max_pages && !IsPageFree(next_free_page_)) next_free_page_++;
      
      page_offset = current_page_offset;
      page_allocated_++;  // 更新已分配页数
      return true;
  }

  LOG(WARNING) << "BUG!!!!!!!" << std::endl;
  return false;
}

/**
 * DONE
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t byte_index = page_offset / 8;  // 计算字节索引
  uint8_t bit_index = page_offset % 8;   // 计算位索引

  // 检查该位是否已经是0（即未分配），如果是，则返回 false
  if ((bytes[byte_index] & (1 << bit_index)) == 0) {
      return false;
  }

  // 清除位，标记页面为未分配
  bytes[byte_index] &= ~(1 << bit_index);
  
  // 只有当页面成功释放时，才减少已分配页面的计数
  page_allocated_--;

  // 更新 next_free_page_，只有当当前释放的页面比记录的 next_free_page_ 更靠前时才更新
  if (page_offset < next_free_page_) {
      next_free_page_ = page_offset;
  }

  return true;
}

/**
 * DONE
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;  // 计算字节索引
  uint8_t bit_index = page_offset % 8;   // 计算位索引
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  unsigned char byte = bytes[byte_index];
  return !(byte & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;

