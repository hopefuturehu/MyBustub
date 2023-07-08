#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto leaf_page = GetLeafPage(key);
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  ValueType v;

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  bool nil = leaf_node->LookUp(key, v, comparator_);
  if(nil){
    result->push_back(v);
    return true;
  }
  return false;
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
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value);
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
  if(IsEmpty()) {
    return;
  }

  auto leaf_page = GetLeafPage(key);
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());

  bool delete_flag = leaf_node->Remove(key, comparator_);
  if(!delete_flag){ // remove failed
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  if(leaf_node->GetSize() >= leaf_node->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }
  CoalesceOrRedistribute(leaf_node);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node) -> bool {
  if(node->IsRootPage()) { //根节点的一些特殊处理
    if(node->IsLeafPage() && node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      return true;
    }
    if(!node->IsLeafPage() && node->GetSize() == 1) {
      auto *root_node = reinterpret_cast<InternalPage *>(node);
      auto root_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));
      auto root_child_node = reinterpret_cast<BPlusTreePage *>(root_child_page->GetData());
      root_child_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = root_child_node->GetPageId();
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(root_child_node->GetPageId(), true);
      return true;
    }
    return false;
  }
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  auto node_index = parent_node->ValueIndex(node->GetPageId());
  //尝试借值
  if(node_index > 0) { //左边
    auto left_bro_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index - 1));
    if(node->IsLeafPage()) {
      auto left_bro_node = reinterpret_cast<LeafPage * >(left_bro_page->GetData());
      auto leaf_node = reinterpret_cast<LeafPage * >(node);
      if(left_bro_node->GetSize() > left_bro_node->GetMinSize()) { 
        left_bro_node->MoveLastToFrontOf(leaf_node);
        parent_node->SetKeyAt(node_index, leaf_node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(left_bro_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        return true;
      }
    } else {
      auto internal_node = reinterpret_cast<InternalPage *>(node);
      auto left_bro_node = reinterpret_cast<InternalPage *>(left_bro_page->GetData());
      if(left_bro_node->GetSize() > left_bro_node->GetMinSize()) {
        left_bro_node->MoveLastToFrontOf(internal_node, parent_node->KeyAt(node_index), buffer_pool_manager_);
        parent_node->SetKeyAt(node_index, internal_node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(left_bro_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        return true;
      }
    }

  } else if (node_index != parent_node->GetSize() - 1) { //右边
    auto right_bro_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index + 1));
    if(node->IsLeafPage()) {
      auto leaf_node = reinterpret_cast<LeafPage *>(node);
      auto right_bro_node = reinterpret_cast<LeafPage * >(right_bro_page->GetData());
      if(right_bro_node->GetSize() > right_bro_node->GetMinSize()) {
        right_bro_node->MoveFirstToEndOf(leaf_node);
        parent_node->SetKeyAt(node_index + 1, right_bro_node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(right_bro_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);        
        return true;
      }
    } else {
      auto internal_node = reinterpret_cast<InternalPage *>(node);
      auto right_bro_node = reinterpret_cast<InternalPage *>(right_bro_page);
      if(right_bro_node->GetSize() > right_bro_node->GetMinSize()) {
        right_bro_node->MoveFirstToEndOf(internal_node, parent_node->KeyAt(node_index + 1), buffer_pool_manager_);
        parent_node->SetKeyAt(node_index + 1, right_bro_node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(right_bro_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);        
        return true;
      }
    }
  }
  // 再尝试合并，统一向左合并
  if(node_index > 0) { //左边
    auto left_bro_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index - 1));
    if(node->IsLeafPage()) {
      auto left_bro_node = reinterpret_cast<LeafPage * >(left_bro_page->GetData());
      auto leaf_node = reinterpret_cast<LeafPage * >(node);
      leaf_node->MoveAllTo(left_bro_node);
    } else {
      auto internal_node = reinterpret_cast<InternalPage *>(node);
      auto left_bro_node = reinterpret_cast<InternalPage *>(left_bro_page->GetData());
      internal_node->MoveAllTo(left_bro_node, parent_node->KeyAt(node_index - 1), buffer_pool_manager_);
    }
    parent_node->Remove(node_index);
    return CoalesceOrRedistribute(parent_node);
  }
  if (node_index != parent_node->GetSize() - 1) { //右边
    auto right_bro_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index + 1));
    if(node->IsLeafPage()) {
      auto leaf_node = reinterpret_cast<LeafPage *>(node);
      auto right_bro_node = reinterpret_cast<LeafPage * >(right_bro_page->GetData());
      right_bro_node->MoveAllTo(leaf_node);
    } else {
      auto internal_node = reinterpret_cast<InternalPage *>(node);
      auto right_bro_node = reinterpret_cast<InternalPage *>(right_bro_page);
      right_bro_node->MoveAllTo(internal_node, parent_node->KeyAt(node_index), buffer_pool_manager_);
    }
    parent_node->Remove(node_index + 1);
    return CoalesceOrRedistribute(parent_node);
  }
  return false;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  if(root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  auto left_leaf = GetLeafPage(KeyType());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, left_leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf_page = GetLeafPage(key);
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->KeyInd(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto root_node = reinterpret_cast<InternalPage* >(root_page->GetData());
  auto key = root_node->KeyAt(root_node->GetSize() - 1);
  auto right_page = GetLeafPage(key);
  auto right_node = reinterpret_cast<LeafPage*>(right_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, right_page, right_node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * CUSTOM
 *****************************************************************************/
/**
 * this method to get the leaf_page to key
 * @return true if find the value of the key, false not find
 * @param v: value of the key
*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE:: GetLeafPage (const KeyType &key) const -> Page * {
  page_id_t leaf_id = root_page_id_;
  while(true) {
    Page *page  = buffer_pool_manager_->FetchPage(leaf_id);
    auto tree_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if(tree_node->IsLeafPage()) {
      return page;
    }
    //此时tree_node 其实是internal_ndoe, 那么就向下遍历
    auto i_node = reinterpret_cast<InternalPage *>(tree_node);
    leaf_id = i_node->LookUp(key, comparator_);
  }
}

/**
 * @brief split the value of node to a new node
 * 
 * @param node origin node to be splited
 * @return N* a ptr to the new node with value
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }

  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->SetPageType(node->GetPageType());

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);

    new_leaf->Init(page->GetPageId(), node->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(new_leaf);
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);
    new_internal->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }

  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value) -> bool{
  Page *leaf_page = GetLeafPage(key);
  auto *page = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType val;

  if(page->LookUp(key, val, comparator_)) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false; // 查找重复值，有重复值那就扔个false回去
  }
  page->Insert(key, value, comparator_);
  if(page->GetSize() >= page->GetMaxSize()){
    //split
    auto new_node = Split(page);
    new_node->SetNextPageId(page->GetNextPageId());
    page->SetNextPageId(new_node->GetPageId());
    auto risen_key = new_node->KeyAt(0); //向上级插入
    InsertIntoParent(page, risen_key, new_node);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node) {
  if(old_node->IsRootPage()) { //根结点满了，那就搞一个新的根结点
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if(page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto *new_root = reinterpret_cast<InternalPage* >(page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }
  // 非根结点上插入走递归方式，不满插入就结束；满了就插入->分裂->InsertIntoParent
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage* >(parent_page);
  if(parent_node->GetSize() < internal_max_size_) { //父节点未满，插就完事了
    parent_node->InsertNodeAt(old_node->GetPageId(),  key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  //满了？分裂！
  auto *node_data = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize() + 1)];
  auto *copy_parent = reinterpret_cast<InternalPage*>(node_data);
  std::memcpy(node_data, parent_page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize()));
  copy_parent->InsertNodeAt(old_node->GetPageId(), key, new_node->GetPageId());
  auto splited_parent = Split(copy_parent);
  KeyType new_key = splited_parent->KeyAt(0);
  std::memcpy(parent_page->GetData(), node_data, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_parent->GetMinSize());
  InsertIntoParent(parent_node, new_key, splited_parent);
  buffer_pool_manager_->UnpinPage(splited_parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  delete[] node_data;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType & value){
  auto buffer_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if(buffer_page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "CAN'T BUILD A NEW TREE");
  }
  auto root_page = reinterpret_cast<LeafPage *>(buffer_page->GetData());
  root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_page ->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  // UpdateRootPageId(1);
}
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
