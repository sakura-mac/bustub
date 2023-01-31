/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node, int index, BufferPoolManager *bpm) : leaf_node_(leaf_node), index_(index), bpm_(bpm){
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    bpm_->UnpinPage(leaf_node_->GetPageId(), false); // will be nullptr, or modify the leaf_node? no(check operator++), 
}  

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_node_->GetNextPageId() == INVALID_PAGE_ID and index_ < leaf_node_->GetSize(); } // no need to check index?

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    return leaf_node_->GetPair(index_); // GetPair should return the reference, otherwise will return nullptr
} // is it too simple?

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    // maybe need to point to next leaf_node
    auto next_page_id = leaf_node_->GetNextPageId();
    if(++index_ >= leaf_node_->GetSize() and next_page_id != INVALID_PAGE_ID){
        bpm_->UnpinPage(leaf_node_->GetPageId(), false);
        auto *leaf_page = bpm_->FetchPage(next_page_id);
        leaf_node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
        index_ = 0;
    }
    // if reach the ), index_ will equal to GetSize()
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool{
    return itr.leaf_node_ == leaf_node_ && itr.index_ == index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool{
    return itr.leaf_node_ != leaf_node_ || itr.index_ != index_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
