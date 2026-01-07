#include "fs_server/lru_cache.hpp"
#include <iostream>
#include <algorithm>

namespace fs_server {

LRUCache::LRUCache(size_t max_cache_size_mb)
    : max_size_(max_cache_size_mb * 1024 * 1024) {
    std::cout << "LRUCache: Initialized with max size: " << max_cache_size_mb << " MB" << std::endl;
}

LRUCache::~LRUCache() {
    Clear();
}

bool LRUCache::Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
                   std::string& out_data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = cache_map_.find(block_uuid);
    if (it == cache_map_.end()) {
        stats_.misses++;
        std::cout << "LRUCache: Cache MISS for block " << block_uuid << std::endl;
        return false;  // Cache miss
    }

    // Cache hit - move to back (most recently used)
    stats_.hits++;
    const std::string& cached_data = it->second.first;
    
    // Validate offset
    if (offset > cached_data.length()) {
        out_data.clear();
        return true;
    }
    
    // Calculate actual read length
    uint32_t actual_length = length;
    if (length == 0) {
        actual_length = cached_data.length() - offset;
    } else {
        uint32_t max_available = cached_data.length() - offset;
        actual_length = std::min(length, max_available);
    }
    
    // Extract requested portion
    out_data = cached_data.substr(offset, actual_length);
    
    // Update access order: move to back
    access_order_.erase(it->second.second);
    access_order_.push_back(block_uuid);
    cache_map_[block_uuid].second = std::prev(access_order_.end());
    
    std::cout << "LRUCache: Cache HIT for block " << block_uuid 
              << " (" << out_data.length() << " bytes)" << std::endl;
    
    return true;  // Cache hit
}

bool LRUCache::Put(uint64_t block_uuid, const std::string& data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // If block already exists, remove it first
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        Remove(block_uuid);
    }
    
    // Check if we need to evict
    while (cache_map_.size() > 0 && 
           // Rough estimate: current_size + new_data_size might exceed max
           (cache_map_.size() * 100000 + data.length() > max_size_)) {
        EvictLRU();
    }
    
    // Add to cache (at back = most recently used)
    access_order_.push_back(block_uuid);
    cache_map_[block_uuid] = {data, std::prev(access_order_.end())};
    
    std::cout << "LRUCache: Cached block " << block_uuid 
              << " (" << data.length() << " bytes)" << std::endl;
    
    return true;
}

bool LRUCache::Remove(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = cache_map_.find(block_uuid);
    if (it == cache_map_.end()) {
        return false;
    }
    
    // Remove from access order list
    access_order_.erase(it->second.second);
    // Remove from map
    cache_map_.erase(it);
    
    return true;
}

bool LRUCache::Contains(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return cache_map_.find(block_uuid) != cache_map_.end();
}

void LRUCache::Clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    access_order_.clear();
    cache_map_.clear();
}

void LRUCache::EvictLRU() {
    // Remove the least recently used (front of list)
    if (!access_order_.empty()) {
        uint64_t lru_uuid = access_order_.front();
        access_order_.pop_front();
        cache_map_.erase(lru_uuid);
        stats_.evictions++;
        std::cout << "LRUCache: Evicted block " << lru_uuid << std::endl;
    }
}

PageCachePolicy::CacheStats LRUCache::GetStats() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Calculate current size
    size_t current_size = 0;
    for (const auto& pair : cache_map_) {
        current_size += pair.second.first.length();
    }
    
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        current_size,
        max_size_,
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
