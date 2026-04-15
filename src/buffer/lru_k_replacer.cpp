#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_frames_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // TODO(student): Implement LRU-K eviction policy
  // - Find the frame with the largest backward k-distance
  // - Among frames with fewer than k accesses, evict the one with earliest first access
  // - Only consider evictable frames
  std::lock_guard<std::mutex> lock(latch_);
  if(curr_size_==0){
    return false;
  }
  bool found_inf = false;
  frame_id_t victim_id=-1;
  size_t min_first_access=std::numeric_limits<size_t>::max();
  size_t min_kth_access=std::numeric_limits<size_t>::max();
  for (auto &pair :entries_){
    frame_id_t current_id=pair.first;
    FrameEntry &entry = pair.second;

    if(!entry.is_evictable_){
      continue;
    }

    size_t oldest_time = entry.history_.front();

// 分类讨论
    if (entry.history_.size() < k_) {
      // 类别 A：访问次数小于 k (+infinity)
      if (oldest_time < min_first_access) {
        min_first_access = oldest_time;
        victim_id = current_id;
        found_inf = true; // 开启了“一票否决”，后面类别 B 再惨我们也不看了
      }
    } else {
      // 类别 B：访问次数达到 k
      // 只有在还没发现任何类别 A 的情况下，类别 B 才有资格参与竞选
      if (!found_inf && oldest_time < min_kth_access) {
        min_kth_access = oldest_time;
        victim_id = current_id;
      }
    }
  }
  if(victim_id!=-1){
    *frame_id=victim_id;
    entries_.erase(victim_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // TODO(student): Record a new access for frame_id at current timestamp
  // - If frame_id is new, create an entry
  // - Add current_timestamp_ to the frame's history
  // - Increment current_timestamp_
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id>=max_frames_){
    throw std::invalid_argument("frame_id is invalid (>= max_frames_)");
  }
  FrameEntry &entry = entries_[frame_id];
  entry.history_.push_back(current_timestamp_);
  if(entry.history_.size()>k_){
    entry.history_.pop_front();
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO(student): Set whether a frame is evictable
  // - Update curr_size_ accordingly
  std::lock_guard<std::mutex> lock(latch_);
  if(frame_id>=max_frames_){
    throw std::invalid_argument("frame_id is invalid");
  }
  auto it = entries_.find(frame_id);
  if(it==entries_.end()){
    return;
  }
  bool is_currently_evictable=it->second.is_evictable_;
  if (is_currently_evictable && !set_evictable){
    curr_size_--;
  }
  else if (!is_currently_evictable && set_evictable){
    curr_size_++;
  }
  it->second.is_evictable_=set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // TODO(student): Remove a frame from the replacer
  // - The frame must be evictable; throw if not
  std::lock_guard<std::mutex> lock(latch_);
  auto it=entries_.find(frame_id);
  if(it==entries_.end()){
    return;
  }
  if (!it->second.is_evictable_) {
    throw std::invalid_argument("Remove called on non-evictable frame");
  }
  curr_size_--;
  entries_.erase(it);
}

auto LRUKReplacer::Size() const -> size_t {
  // TODO(student): Return the number of evictable frames
  return curr_size_;
}

}  // namespace onebase
