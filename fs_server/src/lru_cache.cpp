#include "fs_server/lru_cache.hpp"
#include <iostream>

namespace fs_server {

LRUCache::LRUCache(size_t max_cache_size_mb)
    : max_size_(max_cache_size_mb * 1024 * 1024) {
    std::cout << "LRUCache: Initialized with max size: " << max_cache_size_mb << " MB (placeholder)" << std::endl;
}

LRUCache::~LRUCache() {
}

bool LRUCache::Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
                   std::string& out_data) {
    // Placeholder: Always cache miss
    stats_.misses++;
    std::cout << "LRUCache: Cache MISS for block " << block_uuid << " (placeholder)" << std::endl;
    return false;
}

bool LRUCache::Put(uint64_t block_uuid, const std::string& data) {
    // Placeholder: Accept but don't store
    std::cout << "LRUCache: Put block " << block_uuid << " (placeholder - not storing)" << std::endl;
    return true;
}

bool LRUCache::Remove(uint64_t block_uuid) {
    // Placeholder
    return false;
}

bool LRUCache::Contains(uint64_t block_uuid) {
    // Placeholder
    return false;
}

void LRUCache::Clear() {
    // Placeholder
}

void LRUCache::EvictLRU() {
    // Placeholder
}

PageCachePolicy::CacheStats LRUCache::GetStats() const {
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        0,
        max_size_,
        "LRU"
    };
}

void LRUCache::ResetStats() {
    stats_.hits = 0;
    stats_.misses = 0;
    stats_.evictions = 0;
}

}  // namespace fs_server
