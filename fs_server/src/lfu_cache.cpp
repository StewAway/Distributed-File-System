#include "fs_server/lfu_cache.hpp"
#include <iostream>

namespace fs_server {

LFUCache::LFUCache(size_t max_cache_size_mb)
    : max_size_(max_cache_size_mb * 1024 * 1024) {
    std::cout << "LFUCache: Initialized with max size: " << max_cache_size_mb << " MB (placeholder)" << std::endl;
}

LFUCache::~LFUCache() {
}

bool LFUCache::Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
                   std::string& out_data) {
    // Placeholder: Always cache miss
    stats_.misses++;
    std::cout << "LFUCache: Cache MISS for block " << block_uuid << " (placeholder)" << std::endl;
    return false;
}

bool LFUCache::Put(uint64_t block_uuid, const std::string& data) {
    // Placeholder: Accept but don't store
    std::cout << "LFUCache: Put block " << block_uuid << " (placeholder - not storing)" << std::endl;
    return true;
}

bool LFUCache::Remove(uint64_t block_uuid) {
    // Placeholder
    return false;
}

bool LFUCache::Contains(uint64_t block_uuid) {
    // Placeholder
    return false;
}

void LFUCache::Clear() {
    // Placeholder
}

void LFUCache::EvictLFU() {
    // Placeholder
}

void LFUCache::UpdateFrequency(uint64_t block_uuid) {
    // Placeholder
}

PageCachePolicy::CacheStats LFUCache::GetStats() const {
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        0,
        max_size_,
        "LFU"
    };
}

void LFUCache::ResetStats() {
    stats_.hits = 0;
    stats_.misses = 0;
    stats_.evictions = 0;
}

}  // namespace fs_server
