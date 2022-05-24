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

LRUReplacer::LRUReplacer(size_t num_pages) {};

LRUReplacer::~LRUReplacer() {};

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lock(mtx);

    return false; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mtx);
    auto pair = pageLocator.find(frame_id);
    if(pair == pageLocator.end()){
        return;
    }

    victimList.erase(pair->second);
    pageLocator.erase(pair);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mtx);
    if (pageLocator.count(frame_id) != 0) {
        return;
    }

    victimList.emplace_back(frame_id);
    pageLocator.emplace(frame_id,victimList.begin());
}

size_t LRUReplacer::Size() { return 0; }

}  // namespace bustub
