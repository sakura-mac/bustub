//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // hash the key, find if the corresponding bucket contains the value
  std::scoped_lock<std::mutex> lock(latch_);
  int index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // find the corresponding bucket and remove
  std::scoped_lock<std::mutex> lock(latch_);
  int index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto bucket = dir_[index];  // share_ptr
  // if key can't be inserted
  // cout << "insert key : " << key <<  ", insert index: " << index << endl;
  while (!bucket->Insert(key, value)) {  // is full
    auto list = bucket->GetItems();
    // 1. local_depth euqal to global_depth
    if (global_depth_ == bucket->GetDepth()) {
      // 1.++global_depth
      // 2.double the dir and dir[i+num] = dir[i]
      ++global_depth_;
      size_t old_buckets_num = dir_.size();
      dir_.resize(old_buckets_num * 2);
      for (size_t i = old_buckets_num; i < dir_.size(); i++) {
        dir_[i] = dir_[i - old_buckets_num];
      }
    }

    // 2. both 2 cases should spilit the bucket(increment the local depth) and rehash the items
    // for all pointers, bucket should be spilited into n buckets: hash the index in terms of new_local_depth, same hash
    // point to the same bucket
    auto new_local_depth = bucket->GetDepth() + 1;

    // cout<< "pre num buckets" << num_buckets_ <<  endl;
    // std::unordered_map<size_t, std::shared_ptr<Bucket>> record;
    auto local_hash = [&new_local_depth](size_t &key) {
      int mask = (1 << new_local_depth) - 1;
      return std::hash<size_t>()(key) & mask;
    };
    auto original_bucket_hash = local_hash(index);
    auto ori_bucket = std::make_shared<Bucket>(bucket_size_, new_local_depth);
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, new_local_depth);
    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == bucket) {
        auto hash = local_hash(i);
        if (hash == original_bucket_hash) {
          dir_[i] = ori_bucket;
          // ++num_buckets_;
          // record[hash] = dir_[i];
        } else {
          dir_[i] = new_bucket;
        }
      }
    }
    // --num_buckets_;
    ++num_buckets_;
    // cout << "now num buckets" << num_buckets_ << endl;
    for (auto &[k, v] : list) {  // what if insert is failed?
      auto cur_index = IndexOf(k);
      dir_[cur_index]->Insert(k, v);
    }
    index = IndexOf(key);
    bucket = dir_[index];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // for (auto &[key_, value_] : list_) {
  // if (key_ == key) {
  // value = value_;
  // return true;
  //}
  //}
  // return false;
  return std::any_of(list_.begin(), list_.end(), [&](std::pair<K, V> p) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // for (auto it = list_.begin(); it != list_.end(); it++) {
  // if (it->first == key) {
  // list_.erase(it);
  // return true;
  //}
  //}
  // return false;
  return std::any_of(list_.begin(), list_.end(), [&](std::pair<K, V> p) {
    if (p.first == key) {
      list_.remove(p);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // if not full, insert a pair with key and value
  if (IsFull()) {
    return false;
  }

  // if key exits, overwrite it and return true
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }

  // key not exits, insert [key, value]
  list_.emplace_back(std::pair<K, V>(key, value));

  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
