//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);  //?
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetLSN( max_size/2);	
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*
 * Helper method to get the upper bound of array_
 * @Return : uppper bound from 1 to GetSize()
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(int l, int r, const KeyType &key, KeyComparator &comparator) const
    -> int {
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (comparator(array_[mid].first, key) > 0) {  // is array_[0].first < key ? yes, key must > 0
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}  // OPTIMISE OPTION : in usage, it is equal to LowerBound(never be equal in internal search)

/*
 * Helper method to get the RID associated with key
 * @Return : value(child page id)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindID(const KeyType &key, KeyComparator &comparator) const -> ValueType {
  // find the upper_bound
  int l = 0;
  int r = GetSize();  //[)
  l = UpperBound(l, r, key, comparator);
  // std::cout << "in the internal node: l-1 , value" << l-1 << array_[l-1].second << std::endl;
  return array_[l - 1].second;
}

/*
 * Insert kids to array_
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(std::vector<MappingKeyType> &&vector, KeyComparator &comparator) {
  // append vector to array, for nomal insert(1) or new root(2), OPTIMISE OPTION
  int internal_size = GetSize();
  int insert_size = static_cast<int>(vector.size());
  int end_size = insert_size + internal_size;

  if (insert_size == 1) {
    int l = UpperBound(0, internal_size, vector[0].first, comparator);
    for (int i = internal_size; i > l; i--) {
      array_[i] = array_[i - 1];
    }
    array_[l] = vector[0];
  } else {
    for (int i = internal_size; i < end_size; i++) {
      array_[i] = vector[i - internal_size];
    }
  }
  IncreaseSize(insert_size);
}

/*
 * split the kids from original internal node
 * @Return : kids
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(KeyType *parent_key) -> std::vector<MappingKeyType> {
  std::vector<MappingType> ret;
  // vector[0].first should be empty for parent ...
  int internal_size = GetSize();
  int split_index = internal_size / 2;

  // first slot
  *parent_key = array_[split_index].first;
  auto first_value = array_[split_index].second;
  ret.resize(internal_size - split_index);
  ret[0] = std::move(MappingKeyType(KeyType(), first_value));
  ++split_index;

  // right part to ret
  for (int i = split_index; i < internal_size; i++) {
    ret[i - split_index + 1] = std::move(array_[i]);
  }

  SetSize(split_index - 1);
  return ret;
}

/*
 * find the specific kv pair and return the reference
 * @Return : MappingKeyType &
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPair(int index) -> MappingType & { return array_[index]; }

/*
 * Erase the pair in array
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Erase(int index) {
  for (int i = index; i < GetSize(); i++) {  // if set GetSize( ) - 1, case 0 will fail
    array_[i] = array_[i + 1];
  }
}

/*
 * get the index from child page id
 * @Return : the index from [ 0, size-1]
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexAtValue(page_id_t page_id) -> int {
  // lower bound
  int l = 0;
  int r = GetSize();

  while (l < r) {
    int mid = l + (r - l) / 2;
    if (array_[l].second >= page_id) {  // is array_[0].first < key ? yes, key must > 0
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

/*
 * merge this node into recipient
 * @Return : the index from [ 0, size-1]
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, int erase_index,
                                             BufferPoolManager *bpm) {
  int start = recipient->GetSize();
  page_id_t recip_page_id = recipient->GetPageId();

  // 1. get the parent node
  Page *page = bpm->FetchPage(GetParentPageId());
  auto *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  SetKeyAt(0, parent_node->KeyAt(erase_index));
  bpm->UnpinPage(parent_node->GetPageId(), false);

  // 2. merge the data into recipient
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[start + i] = array_[i];
    // update children's parent page
    auto *child_page = bpm->FetchPage(array_[i].second);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    child_node->SetParentPageId(recip_page_id);
    bpm->UnpinPage(array_[i].second, true);
  }
  // update parent page
  recipient->SetSize(start + GetSize());
  SetSize(0);

  bpm->UnpinPage(GetPageId(), true);
  bpm->UnpinPage(recipient->GetPageId(), true);
}

/*
 * borrow kv from node 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowKIDFrom(B_PLUS_TREE_INTERNAL_PAGE_TYPE *node, int index, KeyType &fill_key){
		// 1. borrow pair and erase
		auto pair = node->GetPair( index);
		node->Erase( index);

		// 2. insert: for 0 index, k

		IncreaseSize( 1);
	
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
