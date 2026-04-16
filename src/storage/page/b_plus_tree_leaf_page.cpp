#include "onebase/storage/page/b_plus_tree_leaf_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeLeafPage<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  // TODO(student): Binary search for the index of key
  //only return the first index i where array_[i].first >= key
  int left=0,right=GetSize();
  while (left<right){
    int mid=left+(right-left)/2;
    if(comparator(array_[mid].first,key)){//mid<key
      left=mid+1;
    }
    else{
      right=mid;
    }
  }
  return left;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                         const KeyComparator &comparator) const -> bool {
  // TODO(student): Look up a key and return its associated value
  //first find the index of the key
  int pos=KeyIndex(key,comparator);
  //then check if the key at that index is equal to the key we are looking for
  if(pos<GetSize()&&!comparator(key,array_[pos].first)&&!comparator(array_[pos].first,key)){
    *value=array_[pos].second;
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                         const KeyComparator &comparator) -> int {
  // TODO(student): Insert a key-value pair in sorted order
  // 1. Find the insertion point using binary search (KeyIndex)
  // 2. If key already exists, do nothing and return current size
  // 3. Shift entries to make room and insert the new key-value pair
  int pos=KeyIndex(key,comparator);//pos is the first index where array_[pos].first >= key

  if(pos<GetSize()&&!comparator(key,array_[pos].first)&&!comparator(array_[pos].first,key)){
    return GetSize();
  }
  for (int i=GetSize();i>pos;i--){
    array_[i]=array_[i-1];
  }
  array_[pos].first=key;
  array_[pos].second=value;
  IncreaseSize(1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                        const KeyComparator &comparator) -> int {
  // TODO(student): Remove a key-value pair
  // 1. Find the index of the key using binary search (KeyIndex)
  int pos=KeyIndex(key,comparator);
  // 2. If key does not exist, do nothing and return current size
  if(pos>=GetSize()||comparator(key,array_[pos].first)||comparator(array_[pos].first,key)){
    return GetSize();
  }
  // 3. Shift entries to fill the gap
  for (int i=pos;i<GetSize()-1;i++){
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move second half of entries to recipient during split
  int split=GetSize()/2;
  int count=GetSize()-split;
  for (int i=0;i<count;i++){
    recipient->array_[i]=array_[split+i];
  }
  recipient->SetNextPageId(next_page_id_);
  recipient->SetSize(count);
  SetSize(split);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move all entries to recipient during merge
  int r_size=recipient->GetSize();
  for (int i=0;i<GetSize();i++){
    recipient->array_[r_size+i]=array_[i];
  }
  recipient->SetNextPageId(next_page_id_);
  recipient->SetSize(r_size+GetSize());
  SetSize(0);

}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move first entry to end of recipient
  int r_size=recipient->GetSize();
  recipient->array_[r_size]=array_[0];
  recipient->IncreaseSize(1);
  for (int i = 0;i<GetSize()-1;i++){
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move last entry to front of recipient
  int r_size=recipient->GetSize();
  for (int i = r_size;i>0;i--){
    recipient->array_[i]=recipient->array_[i-1];
  }
  recipient->array_[0]=array_[GetSize()-1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

}  // namespace onebase
