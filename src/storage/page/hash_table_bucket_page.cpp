//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for(size_t i = 0; i < BUCKET_ARRAY_SIZE; i++){
    if(!IsReadable(i)) {continue;}

    if (cmp(key,array_[i].first) == 0)
    {
      result->push_back(array_[i].second);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  
  int insert_idx = -1;
  
  for(size_t i = 0; i < BUCKET_ARRAY_SIZE; i++){
    if (!IsReadable(i))
    {
      if(insert_idx == -1) {insert_idx = i;}
      continue;
    }

    if (cmp(key,array_[i].first) == 0 && value == array_[i].second)
    {
      return false;
    }
  }

  array_[insert_idx] = MappingType(key,value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for(size_t i = 0; i < BUCKET_ARRAY_SIZE; i++){
      if(!IsReadable(i)){
        continue;
      }
      if (cmp(key,array_[i].first) == 0 && value == array_[i].second)
      {
        RemoveAt(i);
        return true;
      }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if(!IsReadable(bucket_idx)) {  
    return {};
  };
  
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if(!IsReadable(bucket_idx)) {  
    return {};
  };
  
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx/8] &= ~(1<<(bucket_idx%8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return ((occupied_[bucket_idx/8]>>(bucket_idx%8))&1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx/8] |= (1<<(bucket_idx%8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return ((readable_[bucket_idx/8]>>(bucket_idx%8))&1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx/8] |= (1<<(bucket_idx%8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  char full = static_cast<char>((1<<8)-1);
  size_t i = 0;
  for (; i < (BUCKET_ARRAY_SIZE-1) / 8; i++)
  {
    if (readable_[i] != full)
    {
      return false;
    }
  }
  full = static_cast<char>((1<<(BUCKET_ARRAY_SIZE%8==0?8:BUCKET_ARRAY_SIZE%8))-1);
  return readable_[i] == full;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t count = 0;
  for (size_t i = 0; i < (BUCKET_ARRAY_SIZE-1) / 8 + 1; i++)
  {
    uint32_t t_count = 0;
    uint8_t t_i = readable_[i];
    for(t_count = 0; t_i > 0; t_count++){
      t_i &= (t_i -1);
    }
    count += t_count;
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (size_t i = 0; i < (BUCKET_ARRAY_SIZE-1) / 8 + 1; i++)
  {
    if(readable_[i] != 0){
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
