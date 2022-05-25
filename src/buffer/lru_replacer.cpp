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
#define setBit(x,y) x|=(1<<y) //将X的第Y位置1
#define cleanBit(x,y) x&=~(1<<y) //将X的第Y位清0

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages):capacity(num_pages) {};

LRUReplacer::~LRUReplacer() {};

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lock(mtx);
    if (victimList.empty()) {
        return false;
    }
    
    *frame_id = victimList.back();
    pageLocator.erase(*frame_id);
    victimList.pop_back();
    return true; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mtx);
    if (pageLocator.count(frame_id) == 0)
    {
        return;
    }
    
    victimList.erase(pageLocator[frame_id]);
    pageLocator.erase(frame_id);

}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mtx);
    if (pageLocator.count(frame_id) != 0 || Size() >= capacity) {
        return;
    }

    victimList.push_front(frame_id);
    pageLocator[frame_id]=victimList.begin();
}

size_t LRUReplacer::Size() { 
    return victimList.size(); 
}

}  // namespace bustub
