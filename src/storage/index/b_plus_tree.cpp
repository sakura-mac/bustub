#include "storage/index/b_plus_tree.h"
#include <string>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/

/*
 * Search the B+ tree until to find the leaf node based on root_page_id_
 * @Return : leaf node or nullptr
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> LeafPage * {
  auto *root = buffer_pool_manager_->FetchPage(root_page_id_);
  if (root == nullptr) {
    return nullptr;
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(root->GetData());

  // while-loop to find the leaf node
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto child_page_id =
        internal_node->FindID(key, comparator_);  // must have child_page_id ? yes, otherwise will break in while check
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);  // bug fix
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
  }
  return static_cast<LeafPage *>(node);
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 1. find the leaf node from bpm
  LeafPage *leaf_node = FindLeaf(key);
  if (leaf_node == nullptr) {
    return false;
  }

  // 2. find the id from leaf node
  ValueType value;
  bool is_ok = leaf_node->FindID(key, &value, comparator_);

  if (is_ok) {
    result->emplace_back(value);
  }

  // 3. maintain the field in bpm(Unpin the page)
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  return is_ok;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  /* new fucntion:
   * 1. leaf_node->Insert(std::vector<MappingType> && vector, KeyComparator &comparator) -> bool; | return ret true or
   * false; update self and parent_node's(if exists) size(if split, insert first) 1.1 tmp: if leaf_node empty, insert
   * directly, else bsearch to insert single kv
   * 2. leaf_node->Split() -> std::vector<MappingTyep>; | maintain
   * array_ field ,  and return suitable vector for split usage !!!after then need to parent's kv
   * 3. internal_node->Insert(std::vector<MappingType && vector>)->bool;
   * 3.1 tmp: if internal_node empty, insert directly, else ??? i don't know
   * 4. internal_node->Split(KeyType *parent_key) -> std::vector<MappingType>; !!!after then need to parent's kv
   * 5. SplitInternal(); | split internal node up to the root node
   */
  // 1. insert in leaf node
  static int count = 0;
  page_id_t new_leaf_page_id;

  // 1.1 if first insert
  if (IsEmpty()) {  // OPTIMISE OPTION: NewRoot(std::vector<Node *> &node, std::vector<PairType> &&insert_vector, int
                    // type = 0), 0 for leaf 1 for internal, node size will be 0 (leaf root) or 2 (internal root).
                    // insert_vector size will be 1(leaf root) or 2(internal root)
    auto *new_leaf_page = buffer_pool_manager_->NewPage(&new_leaf_page_id);
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
    new_leaf_node->Init(new_leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf_node->Insert(std::move(std::vector<MappingType>{MappingType(key, value)}), comparator_);
    buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);

    root_page_id_ = new_leaf_page_id;
    UpdateRootPageId(false);
    return true;
  }

  // 1.2 nomally insert leaf node
  LeafPage *leaf_node = FindLeaf(key);  // nullptr or leaf node
  if (leaf_node == nullptr) {
    return false;
  }  // for nullptr

  int leaf_size = leaf_node->GetSize();
  auto leaf_page_id = leaf_node->GetPageId();
  auto parent_page_id = leaf_node->GetParentPageId();

  bool ret = leaf_node->Insert(std::move(std::vector<MappingType>{MappingType(key, value)}), comparator_);
  if (!ret) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
  }
  // 2. if need to split leaf node
  if (leaf_size + 1 == leaf_max_size_) {  // OPTIMISE OPTION: SplitLeaf(LeafPage* leaf_node)
    // 2.1 split leaf node
    auto *new_leaf_page = buffer_pool_manager_->NewPage(&new_leaf_page_id);
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
    new_leaf_node->Init(new_leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf_node->Insert(leaf_node->Split(), comparator_);

    // insert into the leaf list
    auto next_page_id = leaf_node->GetNextPageId();
    new_leaf_node->SetNextPageId(next_page_id);
    leaf_node->SetNextPageId(new_leaf_page_id);

    // after split, need to update parent kid
    KeyType new_key = new_leaf_node->KeyAt(0);  // will be insert in parent
                                                // maintain filed after split
    if (parent_page_id !=
        INVALID_PAGE_ID) {  // OPTIMISE OPTION : SetParentKV(Node *node, page_id_t parent_page_id, KeyType &new_key)
      auto *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
      assert(parent_page != nullptr);
      auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

      parent_node->Insert(std::move(std::vector<MappingKeyType>{MappingKeyType(new_key, new_leaf_page_id)}),
                          comparator_);
      new_leaf_node->SetParentPageId(parent_page_id);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
    } else {  // OPTIMISE OPTION: 1. NewRoot(std::vector<Node *> &node, std::vector<PairType> &&insert_vector, int type
              // = 0)
      page_id_t new_root_page_id;
      auto *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
      // maintain the new root field and root page id
      new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      new_root->Insert(std::move(std::vector<MappingKeyType>{MappingKeyType(KeyType(), leaf_page_id),
                                                             MappingKeyType(new_key, new_leaf_page_id)}),
                       comparator_);  // how to init 0 index key?

      leaf_node->SetParentPageId(new_root_page_id);
      new_leaf_node->SetParentPageId(new_root_page_id);
      root_page_id_ = new_root_page_id;
      UpdateRootPageId();  // what's the meaning of 0 ?
      // maintain end
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
      buffer_pool_manager_->UnpinPage(leaf_page_id, true);
      return ret;
    }

    buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);  // bug fix
  }

  // 3. now that has parent internal node, maybe need to split the internal node
  auto *parent_page = buffer_pool_manager_->FetchPage(
      parent_page_id);  // OPTIMISE OPTION: all to function, check first instead of spliting directly
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_node->GetSize() > internal_max_size_) {
    SplitInternal(parent_node);
  } else {
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
  }

  std::string file = std::to_string(++count) + "graph.txt";
  // Draw(buffer_pool_manager_, file);

  assert(GetRootPageId() != INVALID_PAGE_ID);
  return ret;  // if over flow the pool size or page size?
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(
    InternalPage *internal_node) {  // OPTIMISE OPTION: check first instead of split directly
  page_id_t new_internal_page_id;
  page_id_t parent_page_id = internal_node->GetParentPageId();
  page_id_t internal_page_id = internal_node->GetPageId();
  int internal_size = 0;
  // 1. while loop
  do {
    // 1.1 split internal
    auto *new_internal_page =
        buffer_pool_manager_->NewPage(&new_internal_page_id);  // if overflow the pool size? 4  at most(header page,
                                                               // root, specified node ,new node , parent node)
    assert(new_internal_page != nullptr);  // OPTIMISE OPTION: GetNode(page_id_t page_id, int type = 0)
    // OPTMISE OPTION: NewNode(page_id_t page_id, int type = 0)
    auto *new_internal_node = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal_node->Init(new_internal_page_id, INVALID_PAGE_ID, internal_max_size_);

    KeyType parent_key;
    auto split_array = internal_node->Split(&parent_key);

    // need to redistrubute children's parent id
    for (auto &pair : split_array) {  // OPTIMISE OPTION: RedistrubuteParentKV(std::vector<Node> &&split_array,
                                      // page_id_t parent_page_id)
      auto child_page = buffer_pool_manager_->FetchPage(pair.second);
      auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);
      child_node->SetParentPageId(new_internal_page_id);
      buffer_pool_manager_->UnpinPage(pair.second, true);
    }

    // insert the split array
    new_internal_node->Insert(std::move(split_array), comparator_);

    // 1.2 if root , new root as parent
    if (parent_page_id == INVALID_PAGE_ID) {
      page_id_t new_root_page_id;

      // OPTIMISE OPTION: NewRoot(std::vector<Node *> &node, std::vector<PairType> &&insert_vector, int type = 0)
      auto *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
      assert(new_root_page != nullptr);
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
      new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      // maintain the new root field and root page id

      new_root->Insert(std::move(std::vector<MappingKeyType>{MappingKeyType(KeyType(), internal_page_id),
                                                             MappingKeyType(parent_key, new_internal_page_id)}),
                       comparator_);  // how to init 0 index key?

      internal_node->SetParentPageId(new_root_page_id);
      new_internal_node->SetParentPageId(new_root_page_id);
      root_page_id_ = new_root_page_id;
      UpdateRootPageId();  // what's the meaning of 0 ?
      // maintain end
      buffer_pool_manager_->UnpinPage(internal_page_id, true);
      buffer_pool_manager_->UnpinPage(new_internal_page_id, true);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      return;
    }

    // 1.3 maintain other filed
    new_internal_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(new_internal_page_id, true);
    buffer_pool_manager_->UnpinPage(internal_page_id, true);

    // 1.4 now that has parent, update interator and parent's kid
    auto *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    parent_node->Insert(std::move(std::vector<MappingKeyType>{MappingKeyType(parent_key, new_internal_page_id)}),
                        comparator_);
    internal_node = parent_node;
    internal_page_id = parent_page_id;
    internal_size = internal_node->GetSize();
    parent_page_id = internal_node->GetParentPageId();
  } while (internal_size > internal_max_size_);

  buffer_pool_manager_->UnpinPage(internal_page_id, true);  // actually is the parent_node
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  // 1.find the leaf node
  auto *leaf_node = FindLeaf(key);
  assert(leaf_node != nullptr);

  // 2.delete, and check if need to merge or redistrubute
  int leaf_size = leaf_node->GetSize();
  int erase_index = leaf_node->LowerBound(0, leaf_size, key, comparator_);
  assert(erase_index != leaf_max_size_ && comparator_(leaf_node->GetPair(erase_index).first, key) == 0);
  leaf_node->Erase(erase_index);

  if (--leaf_size < leaf_node->GetMinSize() && !leaf_node->IsRootPage()) {
    CheckAfterDOM(leaf_node);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}

/* check if need to merge or borrow kv from sibling
 * DOM means delete or merge
 * @Return : true to merge again, false to ending the procedure
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename Node>
void BPLUSTREE_TYPE::CheckAfterDOM(Node *node) {
  Node *node2 = nullptr;
  // 1. corner case: set leaf as root
  if (node->IsRootPage()) {
    LostRoot(node);
    return;
  }
  // 1. end
  bool is_right = FindRSibling(node, node2);

  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 2. if need to merge
  if (node->GetSize() + node2->GetSize() <= node->GetMaxSize()) {
    // Merge function will merge node2 into node, so need to swap if node2 is in left
    if (!is_right) {
      std::swap(node, node2);
    }
    int remove_idx = parent_node->IndexAtValue(node->GetPageId());
    Merge(node, node2, parent_node, remove_idx);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return;
  }

  // 3. if need to borrow 1 kv: TODO
  // int node_in_parent_index = parent_node->ValueIndex(node->GetPageId());
  BorrowKV(node2, node, is_right);  // unpin node, node2
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
}

/* merge node2 into node1
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename Node>
void BPLUSTREE_TYPE::Merge(Node *node1, Node *node2, InternalPage *parent, int erase_index) {
  assert(node1->GetSize() + node2->GetSize() <= node2->GetMaxSize());

  // 1. get the node
  if (node1->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node1);
    auto *leaf_merge_node = reinterpret_cast<LeafPage *>(node2);

    leaf_merge_node->MergeTo(leaf_node);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node1);
    auto *internal_merge_node = reinterpret_cast<InternalPage *>(node2);

    internal_merge_node->MergeTo(internal_node, erase_index, buffer_pool_manager_);  // TODO(khalilchen):
  }

  // 2. unpin 2 nodes
  page_id_t erase_page_id = node2->GetPageId();
  buffer_pool_manager_->UnpinPage(erase_page_id, true);
  buffer_pool_manager_->DeletePage(erase_page_id);
  buffer_pool_manager_->UnpinPage(node1->GetPageId(), true);

  // 3. delte parent's 1kv and check if need to loop
  parent->Erase(erase_index);
  if (parent->GetSize() <= parent->GetMinSize()) {
    CheckAfterDOM(parent);
  }
}

/*
 * borrow 1 kv from sibling
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename Node>
void BPLUSTREE_TYPE::BorrowKV(Node *sibling_node, Node *node, bool is_right) {
  int borrow_index = sibling_node->GetSize() - 1;
  if (is_right) {
    borrow_index = 0;
  }

	if( node->IsLeafPage( )){
		node->BorrowKVFrom(sibling_node, borrow_index);
	}else{
		KeyType first = KeyType( );
		auto *first_leaf  = FindLeaf(first);
		int first_index = 0;
		KeyType &fill_key = first_leaf->KeyAt( first_index);
		buffer_pool_manager_->UnpinPage( first_leaf->GetPageId( ));

		node->BorrowKIDFrom(sibling_node, borrow_index, &fill_key);
	}

  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
}
/*
 * delete parent and set leaf as root
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename Node>
void BPLUSTREE_TYPE::LostRoot(Node *root) {
  auto *leaf_node = reinterpret_cast<LeafPage *>(
      buffer_pool_manager_->FetchPage(reinterpret_cast<InternalPage *>(root)->GetPair(0).second));
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  buffer_pool_manager_->DeletePage(root_page_id_);

  leaf_node->SetParentPageId(INVALID_PAGE_ID);
  root_page_id_ = leaf_node->GetPageId();
  UpdateRootPageId();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);  // is ok?
}
/*
 * find the sibling and check sibling is left or right
 * @Return : true for right, false for left
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename Node>
auto BPLUSTREE_TYPE::FindRSibling(Node *node, Node *sibling_node) -> bool {
  auto *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  assert(parent_page != nullptr);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int index = parent_node->IndexAtValue(node->GetPageId());
  // 1. choose the sibling: left first
  int sibling_index = index - 1;
  if (index == 0) {
    sibling_index = index + 1;
  }

  sibling_node =
      reinterpret_cast<Node *>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(sibling_index))->GetData());
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);

  return index == 0;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  KeyType left_border = KeyType();  // OPTIMISE OPTION: will KeyType constructor too expensive?
  auto *leaf_begin_node = FindLeaf(left_border);
  return INDEXITERATOR_TYPE(leaf_begin_node, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf_node = FindLeaf(key);
  if (leaf_node == nullptr) {
    throw "buffer pool overflow or empty tree";
  }

  int index = leaf_node->LowerBound(0, leaf_node->GetSize(), key, comparator_);
  assert(comparator_(leaf_node->KeyAt(index), key) == 0);

  return INDEXITERATOR_TYPE(leaf_node, index, buffer_pool_manager_);
}

/**
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {  // the ) end of leaf node, isEnd() is ] end
  KeyType border = KeyType();                       // OPTIMISE OPTION: get the right border instead of the left border
  auto *leaf_node = FindLeaf(border);

  if (leaf_node == nullptr) {
    throw "buffer pool overflow or empty tree";
  }

  while (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf_node->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);

    auto *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    leaf_node = reinterpret_cast<LeafPage *>(next_page->GetData());
  }

  return INDEXITERATOR_TYPE(leaf_node, leaf_node->GetSize(), buffer_pool_manager_);  // auto Unpin
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  std::cout << "root page id: " << root_page_id_ << std::endl;
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
