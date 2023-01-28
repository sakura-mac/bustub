//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

/*
 * Helper method to get the lower bound of the array
 * @Return : lower bound
 */
INDEX_TEMPLATE_ARGUMENTS
inline auto B_PLUS_TREE_LEAF_PAGE_TYPE::LowerBound(int l, int r, const KeyType &key, KeyComparator &comparator) const
    -> int {
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (comparator(array_[mid].first, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

/*
 * Helper method to get the RID associated with key
 * @Return : value(child page id)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindID(const KeyType &key, ValueType *value, KeyComparator &comparator) const -> bool {
  // find the value in order
  int size = GetSize();
  int l = 0;
  int r = size;
  l = LowerBound(l, r, key, comparator);

  // if not find, l will be 0 or size
  if (l != size && comparator(array_[l].first, key) == 0) {
    *value = array_[l].second;
    return true;
  }

  return false;
}

/*
 * in my view, leader should make the decision, crews should return the result
 * so node's insert method should update self field
 * if leaf_node empty, insert directly, else bsearch to insert single kv
 * @Return : duplicate key for false, else for true
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(std::vector<MappingType> &&vector, KeyComparator &comparator) -> bool {
  int leaf_node_size = GetSize();
  int insert_size = vector.size();
  int l = 0;
  // update self kid
  if (leaf_node_size == 0) {
    for (int i = 0; i < insert_size; i++) {
      array_[i] = std::move(vector[i]);
    }

  } else {
    // insert into exists leaf node
    l = LowerBound(l, leaf_node_size, vector[0].first, comparator);
    if (comparator(array_[l].first, vector[0].first) == 0) {
      return false;
    }
    // leave space for insert kid(only one)
    for (int i = leaf_node_size; i > l; i--) {
      array_[i] = array_[i - 1];
    }

    array_[l] = std::move(vector[0]);
  }

  IncreaseSize(insert_size);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split() -> std::vector<MappingType> {
  int leaf_node_size = GetSize();
  int split_index = leaf_node_size / 2;
  // remain the field unspecified, is it ok? modify action will based on leaf_node_size, ok
  std::vector<MappingType> ret(leaf_node_size - split_index);
  for (int i = split_index; i < leaf_node_size; i++) {
    ret[i - split_index] = std::move(array_[i]);
  }
  SetSize(split_index);

  return ret;
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
