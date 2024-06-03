/**
 * @file index_iterator.cpp
 * @brief Implementation of the IndexIterator class.
 */

#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

/**
 * @brief Construct a new IndexIterator object.
 *
 * @param page_id The page id of the leaf page.
 * @param bpm The buffer pool manager.
 * @param index The index of the item in the leaf page.
 */
IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * @brief Dereferences the iterator and returns a pair of GenericKey and RowId.
 * 
 * @return std::pair<GenericKey *, RowId> The pair of GenericKey and RowId.
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}

/**
 * @brief Advances the iterator to the next item.
 * 
 * @return IndexIterator& The reference to the updated iterator.
 */
IndexIterator &IndexIterator::operator++() {
  if(++item_index == page->GetSize() && page->GetNextPageId() != INVALID_PAGE_ID) {
    auto * next_page = reinterpret_cast<::LeafPage *>
        (buffer_pool_manager->FetchPage(page->GetNextPageId())->GetData());
    current_page_id = page->GetNextPageId();
    buffer_pool_manager->UnpinPage(page->GetPageId(), false);
    page = next_page;
    item_index = 0;
  } if(item_index == page->GetSize()) {
    buffer_pool_manager->UnpinPage(current_page_id, false);
    current_page_id = INVALID_PAGE_ID;
    page = nullptr;
    item_index = 0;
    *this = IndexIterator();
  }
  return *this;
}

/**
 * @brief Compares two iterators for equality.
 * 
 * @param itr The iterator to compare with.
 * @return bool True if the iterators are equal, false otherwise.
 */
bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

/**
 * @brief Compares two iterators for inequality.
 * 
 * @param itr The iterator to compare with.
 * @return bool True if the iterators are not equal, false otherwise.
 */
bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}