#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : max_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * DONE
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<mutex> guard(latch_);
  if (lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  frame_set_.erase(*frame_id);
  return true;
}


/**
 * DONE
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_guard<mutex> guard(latch_);
  auto it = frame_set_.find(frame_id);
  if (it != frame_set_.end()) {
    lru_list_.remove(frame_id); // Since std::list does not support fast remove, this operation is O(n)
    frame_set_.erase(it);
  }
}

/**
 * DONE
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<mutex> guard(latch_);
  if (frame_set_.find(frame_id) == frame_set_.end()) {
    lru_list_.push_front(frame_id);
    frame_set_.insert(frame_id);
  }
}

/**
 * DONE
 */
size_t LRUReplacer::Size() {
  lock_guard<mutex> guard(latch_);
  return frame_set_.size();
}






