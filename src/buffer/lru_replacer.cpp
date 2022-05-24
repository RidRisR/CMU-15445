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

LRUReplacer::LRUReplacer(size_t num_pages) {
    this->pageSlotList = 0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { return false; }

void LRUReplacer::Pin(frame_id_t frame_id) {
    cleanBit(this->pageSlotList,pageDict[frame_id]);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    setBit(this->pageSlotList,pageDict[frame_id]);
}

size_t LRUReplacer::Size() { return 0; }

}  // namespace bustub
