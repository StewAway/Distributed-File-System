#include "fs_server/lru_cache.hpp"
#include <iostream>
#include <algorithm>
#include <climits>

namespace fs_server {

LRUCache::LRUCache(size_t max_cache_pages)
    : capacity_(max_cache_pages),
      size_(0) {
    // Initialize sentinel nodes
    head_ = new LinkedListNode();
    tail_ = new LinkedListNode();
    head_->next = tail_;
    tail_->prev = head_;
    std::cout << "LRUCache: Initialized with max pages: " << max_cache_pages << std::endl;
}

LRUCache::~LRUCache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Clean up all nodes
    LinkedListNode* current = head_->next;
    while (current != tail_) {
        LinkedListNode* next = current->next;
        delete current;
        current = next;
    }
    
    delete head_;
    delete tail_;
}

bool LRUCache::Get(uint64_t block_uuid, std::string& out_data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        LinkedListNode* node = cache_map_[block_uuid];
        
        // Move to front (most recently used)
        RemoveNode(node);
        AddNode(node);
        
        // Copy requested data
        out_data = node->page.data;
        stats_.hits++;
        return true;
    }
    
    stats_.misses++;
    return false;
}

bool LRUCache::Put(uint64_t block_uuid, const std::string& data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        // Block already exists, update it
        LinkedListNode* node = cache_map_[block_uuid];
        
        
        node->page.data = data;
        node->page.dirty = true;
        
        // Move to front (most recently used)
        RemoveNode(node);
        AddNode(node);
        
        return true;
    } else {
        // New block, check if we need to evict
        if (size_ >= capacity_) {
            EvictLRU();
        }
        
        // Create new node
        LinkedListNode* newNode = new LinkedListNode(block_uuid, data);
        
        // Add to map and list
        cache_map_[block_uuid] = newNode;
        ++size_;
        
        AddNode(newNode);
        
        return true;
    }
}

bool LRUCache::Remove(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        LinkedListNode* node = cache_map_[block_uuid];
        RemoveNode(node);
        cache_map_.erase(block_uuid);
        --size_;
        delete node;
        return true;
    }
    
    return false;
}

bool LRUCache::Contains(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return cache_map_.find(block_uuid) != cache_map_.end();
}

void LRUCache::Clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Delete all nodes
    LinkedListNode* current = head_->next;
    while (current != tail_) {
        LinkedListNode* next = current->next;
        delete current;
        current = next;
    }
    
    // Reset structures
    head_->next = tail_;
    tail_->prev = head_;
    cache_map_.clear();
    size_ = 0;
}

void LRUCache::RemoveNode(LinkedListNode* node) {
    if (node->prev != nullptr) {
        (node->prev)->next = node->next;
    }
    if (node->next != nullptr) {
        (node->next)->prev = node->prev;
    }
}

void LRUCache::AddNode(LinkedListNode* node) {
    // Add node right after head (most recently used position)
    node->next = head_->next;
    (head_->next)->prev = node;
    
    head_->next = node;
    node->prev = head_;
}

void LRUCache::EvictLRU() {
    // Evict the least recently used (tail->prev)
    if (size_ >= 1) {
        LinkedListNode* lru_node = tail_->prev;
        cache_map_.erase(lru_node->block_uuid);
        
        RemoveNode(lru_node);
        delete lru_node;
        
        --size_;
        ++stats_.evictions;
    }
}

PageCachePolicy::CacheStats LRUCache::GetStats() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        "LRU"
    };
}

void LRUCache::ResetStats() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    stats_.hits = 0;
    stats_.misses = 0;
    stats_.evictions = 0;
}

}  // namespace fs_server
