#include "fs_server/cache.hpp"
#include <iostream>

namespace fs_server {

PageCache::PageCache() {
    std::cout << "PageCache: Initialized (placeholder - always returns cache miss)" << std::endl;
}

PageCache::~PageCache() {
    // Cleanup on destruction
    std::lock_guard<std::mutex> lock(cache_mutex_);
    cache_map_.clear();
}

bool PageCache::Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
                    std::string& out_data) {
    // Placeholder: Always return cache miss for now
    // TODO: Implement actual cache lookup and retrieval
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    stats_.misses++;
    std::cout << "PageCache: Cache MISS for block " << block_uuid << std::endl;
    
    return false;  // Always miss
}

bool PageCache::Put(uint64_t block_uuid, const std::string& data) {
    // Placeholder: Accept write but don't actually store for now
    // TODO: Implement actual cache storage with LRU eviction policy
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    std::cout << "PageCache: Attempted Put for block " << block_uuid 
              << " (placeholder - not storing)" << std::endl;
    
    return true;  // Accept write
}

bool PageCache::Remove(uint64_t block_uuid) {
    // Placeholder implementation
    // TODO: Implement actual block removal
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = cache_map_.find(block_uuid);
    if (it != cache_map_.end()) {
        cache_map_.erase(it);
        return true;
    }
    return false;
}

bool PageCache::Contains(uint64_t block_uuid) {
    // Placeholder: Always return false (never in cache)
    // TODO: Implement actual containment check
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return cache_map_.find(block_uuid) != cache_map_.end();
}

void PageCache::Clear() {
    // Placeholder implementation
    // TODO: Implement actual cache clearing
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    std::cout << "PageCache: Clearing cache" << std::endl;
    cache_map_.clear();
    stats_.misses = 0;
    stats_.hits = 0;
    stats_.evictions = 0;
}

PageCache::CacheStats PageCache::GetStats() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t current_size = 0;
    for (const auto& pair : cache_map_) {
        current_size += pair.second.length();
    }
    
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        current_size,
        0  // max_size - set this when implementing actual cache
    };
}

void PageCache::ResetStats() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    stats_.hits = 0;
    stats_.misses = 0;
    stats_.evictions = 0;
}

}  // namespace fs_server

