//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#define setBit(x, y) x |= (1 << (y))
#define cleanBit(x, y) x &= ~(1 << (y))

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (victim_list_.empty()) {
    return false;
  }

  *frame_id = victim_list_.back();
  page_locator_.erase(*frame_id);
  victim_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (page_locator_.count(frame_id) == 0) {
    return;
  }

  victim_list_.erase(page_locator_[frame_id]);
  page_locator_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (page_locator_.count(frame_id) != 0 || Size() >= capacity_) {
    return;
  }

  victim_list_.push_front(frame_id);
  page_locator_[frame_id] = victim_list_.begin();
}

size_t LRUReplacer::Size() { return victim_list_.size(); }

}  // namespace bustub
