#include "onebase/storage/page/b_plus_tree_internal_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeInternalPage<int, page_id_t, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  // TODO(student): Find the index of the given value in the internal page
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // TODO(student): Find the child page that should contain the given key
  for (int i = GetSize() - 1; i >= 0; i--) {
    if (!comparator(key, array_[i].first)) {
      return array_[i].second;
    }
  }
  return array_[0].second;  // Return the leftmost child if key is smaller than all keys
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) {
  // TODO(student): Create a new root with one key and two children
  array_[0].second = old_value;  // Left child
  array_[1].first = key;         // Key
  array_[1].second = new_value;
  SetSize(2);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) -> int {
  // TODO(student): Insert a new key-value pair after old_value
  int idx=ValueIndex(old_value);
  for (int i=GetSize();i>idx+1;i--){
    array_[i]=array_[i-1];
  }
  array_[idx+1].first=key;
  array_[idx+1].second=new_value;
  IncreaseSize(1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // TODO(student): Remove the key-value pair at the given index
  for (int i =index;i<GetSize()-1;i++){
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  // TODO(student): Remove all entries and return the only remaining child
  ValueType child=array_[0].second;
  SetSize(0);
  return child; 
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move all entries to recipient during merge
  int r_size=recipient->GetSize();
  recipient->array_[r_size].first=middle_key;
  recipient->array_[r_size].second=array_[0].second;
  for (int i=1;i<GetSize();i++){
    recipient->array_[r_size+i]=array_[i];
  }
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move the second half of entries to recipient during split
  int split=GetSize()/2;
  recipient->array_[0].second = array_[split].second;
  int j = 1;
  for (int i = split+1;i<GetSize();i++,j++){
    recipient->array_[j]=array_[i];
  }
  recipient->SetSize(j);
  SetSize(split);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move first entry to end of recipient (redistribute)
  int r_size=recipient->GetSize();
  recipient->array_[r_size].first=middle_key;
  recipient->array_[r_size].second=array_[0].second;
  recipient->IncreaseSize(1);
  for (int i=0;i<GetSize()-1;i++){
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move last entry to front of recipient (redistribute)
  ValueType old_leftmost = recipient->array_[0].second;
  ValueType moved_child = array_[GetSize() - 1].second;

  int r_size=recipient->GetSize();
  for (int i=r_size;i>0;i--){
    recipient->array_[i]=recipient->array_[i-1];
  }
  recipient->array_[1].first=middle_key;
  recipient->array_[0].second=moved_child;
  recipient->array_[1].second = old_leftmost;
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

}  // namespace onebase
