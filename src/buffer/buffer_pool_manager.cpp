#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/common/logger.h"

namespace onebase {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // TODO(student): Allocate a new page in the buffer pool
  // 1. Pick a victim frame from free list or replacer
  // 2. If victim is dirty, write it back to disk
  // 3. Allocate a new page_id via disk_manager_
  // 4. Update page_table_ and page metadata
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;

  //找一个物理空位
  if(!AllocateFrame(&frame_id)){
    return nullptr;
  }

  //要一个全新的逻辑页号
  page_id_t new_page_id=disk_manager_->AllocatePage();
  
  //拿到这个physical frame，把它清空
  Page *page=&pages_[frame_id];
  page->ResetMemory();

  page->page_id_=new_page_id;
  page->pin_count_=1;
  page->is_dirty_=false;

  page_table_[new_page_id]=frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  *page_id=new_page_id;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  // TODO(student): Fetch a page from the buffer pool
  // 1. Search page_table_ for existing mapping
  // 2. If not found, pick a victim frame
  // 3. Read page from disk into the frame
    std::lock_guard<std::mutex> lock(latch_);
  //检查是否已经在内存里，如果是，直接增加引用计数，更新LRU-K
  if(page_table_.find(page_id)!=page_table_.end()){
    frame_id_t frame_id=page_table_[page_id];
    Page *page=&pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }
  //如果不在内存里，需要一个空位
  frame_id_t frame_id;
  if(!AllocateFrame(&frame_id)){
    return nullptr;//没位置了，且无法淘汰
  }
  //有了位置后，读出位置里的数据
  Page *page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id,page->data_);
  //更新页面元数据
  page->page_id_=page_id;
  page->pin_count_=1;
  page->is_dirty_=false;

  page_table_[page_id]=frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  // TODO(student): Unpin a page, decrementing pin count
  // - If pin_count reaches 0, set evictable in replacer
  std::lock_guard<std::mutex> lock(latch_);

  //如果不在内存里，直接返回false
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()){
    return false;
  }
  frame_id_t frame_id=it->second;
  Page *page=&pages_[frame_id];

  if (page->pin_count_ <= 0) {
    return false;
  }

  if(is_dirty){
    page->is_dirty_=true;
  }

  page->pin_count_--;

  if(page->pin_count_==0){
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // TODO(student): Delete a page from the buffer pool
  // - Page must have pin_count == 0
  // - Remove from page_table_, reset memory, add frame to free_list_
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()){
    return true;
  }
  frame_id_t frame_id=it->second;
  Page *page = &pages_[frame_id];

  if(page->pin_count_>0){
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page->ResetMemory();
  page->page_id_=INVALID_PAGE_ID;
  page->is_dirty_=false;
  page->pin_count_=0;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // TODO(student): Force flush a page to disk regardless of dirty flag
  std::lock_guard<std::mutex> lock(latch_);
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()){
    return false;
  }
  frame_id_t frame_id=it->second;
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id,page->GetData());
  page->is_dirty_=false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // TODO(student): Flush all pages in the buffer pool to disk
  std::lock_guard<std::mutex> lock(latch_);
  
  // 遍历哈希表里所有在内存中的页面，全部刷盘
  for (auto const& [page_id, frame_id] : page_table_) {
    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::AllocateFrame(frame_id_t *frame_id)-> bool {
  // 1. 先去 free_list_ 里找。如果还有完全没用过的空位，直接拿走
  if(!free_list_.empty()){
    *frame_id=free_list_.front();
    free_list_.pop_front();
    return true;
  }

  // 2. 如果 free_list_ 空了，说明内存满了，去求助 LRU-K
  if(replacer_->Evict(frame_id)){
    //成功提出，其物理位置就是 *frame_id
    Page *victim_page = &pages_[*frame_id];

    if(victim_page->IsDirty()){
      disk_manager_->WritePage(victim_page->GetPageId(),victim_page->GetData());
      victim_page->is_dirty_=false;
    }
    page_table_.erase(victim_page->GetPageId());
    return true;
  }
  return false;
}
}  // namespace onebase
