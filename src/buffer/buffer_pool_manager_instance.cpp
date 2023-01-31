//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  //  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // if not exists: INVALID_PAGE_ID check is faster in case of lots of invalid page ids
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // flush in disk, set dirty flag regardless of original flag
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  // for-loop flushpgimp
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  // allocate new page
  // new page id
  // new frame id
  // maintain the corresponding field: page_table_, replacer_(hash table and lru-k list), pages_(working space)
  int new_page_id = AllocatePage();
  *page_id = new_page_id;
  frame_id_t frame_id = FindNewFrame();
  if (frame_id == -1) {
    return nullptr;
  }
  page_table_->Insert(new_page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  InitNewPage(frame_id, new_page_id);
  //std::cout << "new pin page " << std::endl;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  // if page in cache, find the page and lock it
  // if not find, read from disk(disk_manager) to new clean frame, maintaining page_table, pages, replacer

  // bugs in corresponding filed: replacer, because 'lock it' means unevictable(in terms of pin_count)
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    ++pages_[frame_id].pin_count_;
    //std::cout << "fpin page" << std::endl;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }

  frame_id_t new_frame_id = FindNewFrame();
  if (new_frame_id == -1) {
    return nullptr;
  }
  page_table_->Insert(page_id, new_frame_id);
  InitNewPage(new_frame_id, page_id);
  replacer_->RecordAccess(new_frame_id);
  disk_manager_->ReadPage(page_id, pages_[new_frame_id].GetData());
  return &pages_[new_frame_id];
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // if not in page_table_, return true
  // if pin_count == 0, return false
  // before delete, need to check out if is dirty
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }

  // maintaining the fields: page_talble, pages, replacer, free_list
  page_table_->Remove(page_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();  // clear the page data
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // if not exist in buffer pool(not proper function in replacer, so matching the  page_table's function)
  // maintain fields: pin_count, evictable, is_dirty
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ == 0) {
    //std::cout << "oh no" << std::endl;
    return false;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;  // bug fix
  //std::cout << "unpin : pin count: " << pages_[frame_id].pin_count_ << std::endl;
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;  // initial value is 0
}

auto BufferPoolManagerInstance::FindNewFrame() -> frame_id_t {
  // first check the free_list, else check cache

  // for test lab2 checkpoint1 bug
  //std::cout << "find new frame: ";
  //int count = 0;
  //for(size_t i = 0; i < pool_size_; i++){
    //if(pages_[i].pin_count_ > 0){
      //std::cout << i << " ";
      //count++;
    //}
  //}
  //std::cout << ". there are " << count << "pin pages" << std::endl;
  // for test lab2 checkpoint1 bug

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // if mapping is unevictable
    if (!replacer_->Evict(&frame_id)) {
      return -1;
    }
    // before replace, check if need to flush in disk
    page_id_t page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
    page_table_->Remove(page_id);
  }
  return frame_id;
}

void BufferPoolManagerInstance::InitNewPage(frame_id_t frame_id, page_id_t page_id) {
  // maintaining the fields: page_id, pin_count, is_dirty
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
}

}  // namespace bustub
