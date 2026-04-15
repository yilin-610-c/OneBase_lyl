#include "onebase/buffer/page_guard.h"
#include "onebase/common/exception.h"

namespace onebase {

// --- BasicPageGuard ---

BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page)
    : bpm_(bpm), page_(page) {}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) -> BasicPageGuard & {
  // TODO(student): Implement move assignment
  // - Drop current page if held, then take ownership of `that`
  // 1. 防止自我赋值 (比如 guard = std::move(guard))
  if (this != &that) {
    // 2. 释放当前持有的资源
    Drop();
    
    // 3. 接管 that 的所有权
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    
    // 4. 将 that 掏空 (极其重要，防止 that 析构时触发错误的 Unpin)
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

auto BasicPageGuard::GetPageId() const -> page_id_t {
  return page_ ? page_->GetPageId() : INVALID_PAGE_ID;
}

auto BasicPageGuard::GetData() const -> const char * { return page_->GetData(); }
auto BasicPageGuard::GetDataMut() -> char * {
  is_dirty_ = true;
  return page_->GetData();
}

auto BasicPageGuard::IsDirty() const -> bool { return is_dirty_; }

void BasicPageGuard::Drop() {
  if (page_ == nullptr) { return; }
  // TODO(student): Unpin the page via BPM and reset state
  // - Call bpm_->UnpinPage(page_id, is_dirty_) if page_ is not null
  // - Set bpm_ and page_ to nullptr
  // 如果当前没有持有页面，直接返回
  if (page_ != nullptr && bpm_ != nullptr) {
    // 释放资源：调用大管家的 UnpinPage
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  // 重置状态
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

// --- ReadPageGuard ---

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page)
    : bpm_(bpm), page_(page) {
  if (page_) { page_->RLatch(); }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) -> ReadPageGuard & {
  // TODO(student): Implement move assignment (drop current, take that)
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
  }
  return *this;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

auto ReadPageGuard::GetPageId() const -> page_id_t {
  return page_ ? page_->GetPageId() : INVALID_PAGE_ID;
}

auto ReadPageGuard::GetData() const -> const char * { return page_->GetData(); }

void ReadPageGuard::Drop() {
  if (page_ == nullptr) { return; }
  // TODO(student): Release read latch, unpin page, reset state
  if (page_ != nullptr && bpm_ != nullptr) {
    // 1. 释放读锁
    page_->RUnlatch();
    // 2. Unpin 页面，读操作不脏页，传 false
    bpm_->UnpinPage(page_->GetPageId(), false);
  }
  bpm_ = nullptr;
  page_ = nullptr;
}

// --- WritePageGuard ---

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page)
    : bpm_(bpm), page_(page) {
  if (page_) { page_->WLatch(); }
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) -> WritePageGuard & {
  // TODO(student): Implement move assignment (drop current, take that)
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
  }
  return *this;
}

WritePageGuard::~WritePageGuard() { Drop(); }

auto WritePageGuard::GetPageId() const -> page_id_t {
  return page_ ? page_->GetPageId() : INVALID_PAGE_ID;
}

auto WritePageGuard::GetData() const -> const char * { return page_->GetData(); }
auto WritePageGuard::GetDataMut() -> char * { return page_->GetData(); }

void WritePageGuard::Drop() {
  if (page_ == nullptr) { return; }
  // TODO(student): Release write latch, unpin page (dirty=true), reset state
  if (page_ != nullptr && bpm_ != nullptr) {
    // 1. 释放写锁
    page_->WUnlatch();
    // 2. Unpin 页面，写操作使页面变脏，传 true
    bpm_->UnpinPage(page_->GetPageId(), true);
  }
  bpm_ = nullptr;
  page_ = nullptr;
}

}  // namespace onebase
