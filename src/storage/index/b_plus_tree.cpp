#include "onebase/storage/index/b_plus_tree.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTree<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *bpm, const KeyComparator &comparator,
                           int leaf_max_size, int internal_max_size)
    : Index(std::move(name)), bpm_(bpm), comparator_(comparator),
      leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size) {
  if (leaf_max_size_ == 0) {
    leaf_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage) - sizeof(page_id_t)) /
        (sizeof(KeyType) + sizeof(ValueType)));
  }
  if (internal_max_size_ == 0) {
    internal_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage)) /
        (sizeof(KeyType) + sizeof(page_id_t)));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // TODO(student): Insert a key-value pair into the B+ tree
  // 1. If tree is empty, create a new leaf root
  if (IsEmpty()) {
    page_id_t root_id = INVALID_PAGE_ID;
    Page *root_page = bpm_->NewPage(&root_id);//create a new page for the root
    if (root_page == nullptr) {
      return false;
    }
    // Initialize the root page as a leaf page and insert the key-value pair
    // root_page->GetData() returns a pointer to the page's data, which we can cast to a LeafPage pointer
    auto *root_leaf = reinterpret_cast<LeafPage *>(root_page->GetData());
    root_leaf->Init(leaf_max_size_);
    root_leaf->SetParentPageId(INVALID_PAGE_ID);
    root_leaf->Insert(key, value, comparator_);
    root_page_id_ = root_id;
    // Unpin the root page and mark it as dirty since we modified it
    bpm_->UnpinPage(root_id, true);
    return true;
  }

  // 2. Find the leaf page to insert the key-value pair
  auto find_leaf = [&](const KeyType &search_key) -> Page * {
    // Start from the root page and traverse down to the leaf page
    Page *page = bpm_->FetchPage(root_page_id_);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!node->IsLeafPage()) {
      // Use the internal page's Lookup function to find 
      // the child page that should contain the search key
      auto *internal = reinterpret_cast<InternalPage *>(node);
      page_id_t child_id = internal->Lookup(search_key, comparator_);
      bpm_->UnpinPage(page->GetPageId(), false);
      page = bpm_->FetchPage(child_id);
      node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    return page;
  };

  // Find the leaf page where the key-value pair should be inserted
  Page *leaf_page = find_leaf(key);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf->GetSize();
  int new_size = leaf->Insert(key, value, comparator_);

  // If key already exists, do nothing and return false
  if (new_size == old_size) {
    bpm_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  // If the leaf page has enough space, unpin it and return true
  if (new_size <= leaf->GetMaxSize()) {
    bpm_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // If the leaf page is full, split it and insert the new key-value pair
  // 新建一个右叶子，把原来叶子的一半数据搬到右叶子，更新原来叶子的next_page_id_指向新叶子
  page_id_t new_leaf_id = INVALID_PAGE_ID;
  Page *new_leaf_page = bpm_->NewPage(&new_leaf_id);
  // If we cannot allocate a new page for the split, unpin the original leaf page and return false
  if (new_leaf_page == nullptr) {
    bpm_->UnpinPage(leaf_page->GetPageId(), true);
    return false;
  }

  // Initialize the new leaf page and move half of 
  // the entries from the original leaf to the new leaf
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
  new_leaf->Init(leaf_max_size_);
  new_leaf->SetParentPageId(leaf->GetParentPageId());//新叶子和原来叶子有相同的父节点
  leaf->MoveHalfTo(new_leaf);
  leaf->SetNextPageId(new_leaf_id);//原来叶子指向新叶子
  // The first key of the new leaf will be the middle key that 
  // needs to be pushed up to the parent internal page
  KeyType middle_key = new_leaf->KeyAt(0);

  // Insert the middle key and the new leaf page id into the parent internal page
  std::function<void(Page *, const KeyType &, Page *)> insert_into_parent;
  insert_into_parent = [&](Page *old_page, const KeyType &push_up_key, Page *new_page) {
    auto *old_node = reinterpret_cast<BPlusTreePage *>(old_page->GetData());
    auto *new_node = reinterpret_cast<BPlusTreePage *>(new_page->GetData());

    // If the old node is the root page, we need to create 
    // a new root internal page
    if (old_node->IsRootPage()) {
      page_id_t new_root_id = INVALID_PAGE_ID;
      Page *new_root_page = bpm_->NewPage(&new_root_id);
      if (new_root_page == nullptr) {
        bpm_->UnpinPage(old_page->GetPageId(), true);
        bpm_->UnpinPage(new_page->GetPageId(), true);
        return;
      }
      // Initialize the new root page and set the old root 
      // and new leaf as its children
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
      new_root->Init(internal_max_size_);
      new_root->SetParentPageId(INVALID_PAGE_ID);
      //左子是原来叶子，右子是新叶子，中间键是新叶子的第一个键
      new_root->PopulateNewRoot(old_page->GetPageId(), push_up_key, new_page->GetPageId());

      // Update the parent page id of the old root and 
      // new leaf to the new root
      old_node->SetParentPageId(new_root_id);
      new_node->SetParentPageId(new_root_id);
      root_page_id_ = new_root_id;

      bpm_->UnpinPage(old_page->GetPageId(), true);
      bpm_->UnpinPage(new_page->GetPageId(), true);
      bpm_->UnpinPage(new_root_id, true);
      return;
    }

    // If the old node is not the root page, we need to insert 
    // the middle key and new page id into the parent internal page
    page_id_t parent_id = old_node->GetParentPageId();
    Page *parent_page = bpm_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    new_node->SetParentPageId(parent_id);
    //父节点多了一个键值对，插入到父节点中
    parent->InsertNodeAfter(old_page->GetPageId(), push_up_key, new_page->GetPageId());
    //如果父节点没有满，直接返回
    if (parent->GetSize() <= parent->GetMaxSize()) {
      bpm_->UnpinPage(old_page->GetPageId(), true);
      bpm_->UnpinPage(new_page->GetPageId(), true);
      bpm_->UnpinPage(parent_id, true);
      return;
    }
    //如果父节点满了，继续分裂父节点
    int mid_idx = parent->GetSize() / 2;
    KeyType parent_middle_key = parent->KeyAt(mid_idx);
    // 新建一个右内部节点
    page_id_t new_internal_id = INVALID_PAGE_ID;
    Page *new_internal_page = bpm_->NewPage(&new_internal_id);
    if (new_internal_page == nullptr) {
      bpm_->UnpinPage(old_page->GetPageId(), true);
      bpm_->UnpinPage(new_page->GetPageId(), true);
      bpm_->UnpinPage(parent_id, true);
      return;
    }

    auto *new_internal = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal->Init(internal_max_size_);
    new_internal->SetParentPageId(parent->GetParentPageId());
    // 分裂父节点，把父节点的一半键值对搬到新内部节点，更新父节点的size
    parent->MoveHalfTo(new_internal, parent_middle_key);

    // 更新新内部节点的子节点的父节点id为新内部节点的id
    for (int i = 0; i < new_internal->GetSize(); i++) {//遍历新内部节点的每个键值对
      page_id_t child_id = new_internal->ValueAt(i);
      Page *child_page = bpm_->FetchPage(child_id);//拿到子节点的page
      if (child_page != nullptr) {
        auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child->SetParentPageId(new_internal_id);//更新子节点的父节点id为新内部节点的id
        bpm_->UnpinPage(child_id, true);
      }
    }

    insert_into_parent(parent_page, parent_middle_key, new_internal_page);
  };

  insert_into_parent(leaf_page, middle_key, new_leaf_page);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if (IsEmpty()) {
    return;
  }

  auto find_leaf = [&](const KeyType &search_key) -> Page * {
    Page *page = bpm_->FetchPage(root_page_id_);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!node->IsLeafPage()) {
      auto *internal = reinterpret_cast<InternalPage *>(node);
      page_id_t child_id = internal->Lookup(search_key, comparator_);
      bpm_->UnpinPage(page->GetPageId(), false);
      page = bpm_->FetchPage(child_id);
      node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    return page;
  };

  Page *leaf_page = find_leaf(key);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, comparator_);

  if (new_size == old_size) {
    bpm_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }

  if (leaf->IsRootPage()) {
    if (new_size == 0) {
      page_id_t old_root_id = leaf_page->GetPageId();
      bpm_->UnpinPage(old_root_id, true);
      bpm_->DeletePage(old_root_id);
      root_page_id_ = INVALID_PAGE_ID;
    } else {
      bpm_->UnpinPage(leaf_page->GetPageId(), true);
    }
    return;
  }

  if (new_size >= leaf->GetMinSize()) {
    bpm_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }

  std::function<void(Page *)> coalesce_or_redistribute;
  coalesce_or_redistribute = [&](Page *page) {
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    if (node->IsRootPage()) {
      if (!node->IsLeafPage() && node->GetSize() == 1) {
        auto *root = reinterpret_cast<InternalPage *>(node);
        page_id_t only_child_id = root->RemoveAndReturnOnlyChild();
        Page *child_page = bpm_->FetchPage(only_child_id);
        if (child_page != nullptr) {
          auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          child->SetParentPageId(INVALID_PAGE_ID);
          bpm_->UnpinPage(only_child_id, true);
        }
        root_page_id_ = only_child_id;
        page_id_t old_root_id = page->GetPageId();
        bpm_->UnpinPage(old_root_id, true);
        bpm_->DeletePage(old_root_id);
      } else {
        bpm_->UnpinPage(page->GetPageId(), true);
      }
      return;
    }

    page_id_t parent_id = node->GetParentPageId();
    Page *parent_page = bpm_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    int node_index = parent->ValueIndex(page->GetPageId());
    bool sibling_is_left = node_index > 0;
    int sibling_index = sibling_is_left ? (node_index - 1) : (node_index + 1);
    page_id_t sibling_id = parent->ValueAt(sibling_index);
    Page *sibling_page = bpm_->FetchPage(sibling_id);
    auto *sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());

    int parent_key_index = sibling_is_left ? node_index : sibling_index;
    KeyType parent_key = parent->KeyAt(parent_key_index);

    if (node->IsLeafPage()) {
      auto *node_leaf = reinterpret_cast<LeafPage *>(node);
      auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_node);

      if (node_leaf->GetSize() + sibling_leaf->GetSize() <= node_leaf->GetMaxSize()) {
        LeafPage *left = nullptr;
        LeafPage *right = nullptr;
        Page *left_page = nullptr;
        Page *right_page = nullptr;
        if (sibling_is_left) {
          left = sibling_leaf;
          left_page = sibling_page;
          right = node_leaf;
          right_page = page;
        } else {
          left = node_leaf;
          left_page = page;
          right = sibling_leaf;
          right_page = sibling_page;
        }

        right->MoveAllTo(left);
        int remove_index = parent->ValueIndex(right_page->GetPageId());
        parent->Remove(remove_index);

        bpm_->UnpinPage(left_page->GetPageId(), true);
        page_id_t right_id = right_page->GetPageId();
        bpm_->UnpinPage(right_id, true);
        bpm_->DeletePage(right_id);

        if (parent->GetSize() < parent->GetMinSize()) {
          coalesce_or_redistribute(parent_page);
        } else {
          bpm_->UnpinPage(parent_id, true);
        }
      } else {
        if (sibling_is_left) {
          sibling_leaf->MoveLastToFrontOf(node_leaf);
          parent->SetKeyAt(parent_key_index, node_leaf->KeyAt(0));
        } else {
          sibling_leaf->MoveFirstToEndOf(node_leaf);
          parent->SetKeyAt(parent_key_index, sibling_leaf->KeyAt(0));
        }
        bpm_->UnpinPage(page->GetPageId(), true);
        bpm_->UnpinPage(sibling_id, true);
        bpm_->UnpinPage(parent_id, true);
      }
      return;
    }

    auto *node_internal = reinterpret_cast<InternalPage *>(node);
    auto *sibling_internal = reinterpret_cast<InternalPage *>(sibling_node);
    if (node_internal->GetSize() + sibling_internal->GetSize() <= node_internal->GetMaxSize()) {
      InternalPage *left = nullptr;
      InternalPage *right = nullptr;
      Page *left_page = nullptr;
      Page *right_page = nullptr;
      if (sibling_is_left) {
        left = sibling_internal;
        left_page = sibling_page;
        right = node_internal;
        right_page = page;
      } else {
        left = node_internal;
        left_page = page;
        right = sibling_internal;
        right_page = sibling_page;
      }

      right->MoveAllTo(left, parent_key);
      for (int i = 0; i < left->GetSize(); i++) {
        page_id_t child_id = left->ValueAt(i);
        Page *child_page = bpm_->FetchPage(child_id);
        if (child_page != nullptr) {
          auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          child->SetParentPageId(left_page->GetPageId());
          bpm_->UnpinPage(child_id, true);
        }
      }

      int remove_index = parent->ValueIndex(right_page->GetPageId());
      parent->Remove(remove_index);

      bpm_->UnpinPage(left_page->GetPageId(), true);
      page_id_t right_id = right_page->GetPageId();
      bpm_->UnpinPage(right_id, true);
      bpm_->DeletePage(right_id);

      if (parent->GetSize() < parent->GetMinSize()) {
        coalesce_or_redistribute(parent_page);
      } else {
        bpm_->UnpinPage(parent_id, true);
      }
      return;
    }

    if (sibling_is_left) {
      KeyType new_parent_key = sibling_internal->KeyAt(sibling_internal->GetSize() - 1);
      sibling_internal->MoveLastToFrontOf(node_internal, parent_key);
      page_id_t moved_child_id = node_internal->ValueAt(0);
      Page *moved_child_page = bpm_->FetchPage(moved_child_id);
      if (moved_child_page != nullptr) {
        auto *moved_child = reinterpret_cast<BPlusTreePage *>(moved_child_page->GetData());
        moved_child->SetParentPageId(page->GetPageId());
        bpm_->UnpinPage(moved_child_id, true);
      }
      parent->SetKeyAt(parent_key_index, new_parent_key);
    } else {
      KeyType new_parent_key = sibling_internal->KeyAt(1);
      sibling_internal->MoveFirstToEndOf(node_internal, parent_key);
      page_id_t moved_child_id = node_internal->ValueAt(node_internal->GetSize() - 1);
      Page *moved_child_page = bpm_->FetchPage(moved_child_id);
      if (moved_child_page != nullptr) {
        auto *moved_child = reinterpret_cast<BPlusTreePage *>(moved_child_page->GetData());
        moved_child->SetParentPageId(page->GetPageId());
        bpm_->UnpinPage(moved_child_id, true);
      }
      parent->SetKeyAt(parent_key_index, new_parent_key);
    }

    bpm_->UnpinPage(page->GetPageId(), true);
    bpm_->UnpinPage(sibling_id, true);
    bpm_->UnpinPage(parent_id, true);
  };

  coalesce_or_redistribute(leaf_page);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if (IsEmpty()) {
    return false;
  }

  Page *page = bpm_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_id = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(page->GetPageId(), false);
    page = bpm_->FetchPage(child_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(node);
  ValueType value{};
  bool found = leaf->Lookup(key, &value, comparator_);
  bpm_->UnpinPage(page->GetPageId(), false);
  if (found) {
    result->push_back(value);
  }
  return found;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin() -> Iterator {
  if (IsEmpty()) {
    return Iterator(INVALID_PAGE_ID, 0, bpm_);
  }

  Page *page = bpm_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_id = internal->ValueAt(0);
    bpm_->UnpinPage(page->GetPageId(), false);
    page = bpm_->FetchPage(child_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  page_id_t first_leaf_id = page->GetPageId();
  bpm_->UnpinPage(first_leaf_id, false);
  return Iterator(first_leaf_id, 0, bpm_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> Iterator {
  if (IsEmpty()) {
    return Iterator(INVALID_PAGE_ID, 0, bpm_);
  }

  Page *page = bpm_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_id = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(page->GetPageId(), false);
    page = bpm_->FetchPage(child_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(node);
  int index = leaf->KeyIndex(key, comparator_);
  page_id_t leaf_id = page->GetPageId();
  bpm_->UnpinPage(leaf_id, false);
  return Iterator(leaf_id, index, bpm_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::End() -> Iterator {
  return Iterator(INVALID_PAGE_ID, 0);
}

}  // namespace onebase
