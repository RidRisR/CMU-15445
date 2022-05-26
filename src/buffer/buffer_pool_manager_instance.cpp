//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

#include "common/logger.h"

#include <assert.h>

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  assert(page_id == pages_[frame_id].GetPageId());
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for(auto kv: page_table_){
    FlushPgImp(kv.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);

  *page_id = AllocatePage();
  Page* newPage = nullptr;

  frame_id_t frame_id = replacePage_locked(*page_id,newPage);
  if(frame_id < 0){
    return newPage;
  }

  newPage->page_id_ = *page_id;
  newPage->ResetMemory();
  newPage->is_dirty_ = true;
  newPage->pin_count_ += 1;
  page_table_[*page_id] = frame_id;
  // LOG_DEBUG("page_id: %d; page_table: %d", *page_id,pages_[frame_id]);
  return newPage;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);

  Page* fetchedPage = nullptr;

  if (page_table_.count(page_id) > 0){
    fetchedPage = &pages_[page_table_[page_id]];
  }else{
    frame_id_t frame_id = replacePage_locked(page_id,fetchedPage);
    if(frame_id < 0){ return nullptr; }
    updatePage_locked(*fetchedPage, page_id);
    page_table_[page_id] = frame_id;
    assert(page_id == pages_[frame_id].GetPageId());
  }

  replacer_->Pin(page_table_[page_id]);
  fetchedPage->pin_count_ += 1;
  return fetchedPage;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  if(page_table_.count(page_id) == 0){
    DeallocatePage(page_id);
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if(pages_[frame_id].GetPinCount() != 0){
    return false;
  }
  
  assert(page_id == pages_[frame_id].GetPageId());
  replacer_->Pin(frame_id);
  page_table_.erase(page_id);
  free_list_.push_front(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  std::lock_guard<std::mutex> lock(latch_);
  auto pair = page_table_.find(page_id);
  if(pair == page_table_.end()){
    return true;
  }

  Page* target = &pages_[pair->second];
  if(target->pin_count_ <= 0){
      return false; 
  }

  target->pin_count_ -= 1;
  replacer_->Unpin(pair->second);
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

void BufferPoolManagerInstance::updatePage_locked(Page &p, page_id_t page_id) {
    disk_manager_->ReadPage(page_id, p.data_);
    p.page_id_ = page_id;
}

frame_id_t BufferPoolManagerInstance::replacePage_locked(page_id_t page_id, Page* &newPage){
  
  frame_id_t frame_id;
  if (!free_list_.empty())
  {
    frame_id = *free_list_.begin();
    free_list_.pop_front();

    newPage = &pages_[frame_id];
  }
  else if(!replacer_->Victim(&frame_id)){
      frame_id = -1;
  }else{
    Page& victim = pages_[frame_id];
    if(victim.is_dirty_){
      disk_manager_->WritePage(victim.GetPageId(),victim.GetData());
      victim.is_dirty_ = false;
    }
    page_table_.erase(victim.GetPageId());

    newPage = &victim;
  }
  
  return frame_id;
}

}  // namespace bustub


