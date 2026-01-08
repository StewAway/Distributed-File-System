#include "fs_server/cache.hpp"
#include "fs_server/lru_cache.hpp"
#include "fs_server/lfu_cache.hpp"
#include <iostream>

namespace fs_server {

PageCache::PageCache(CachePolicy policy, size_t max_cache_size_mb) {
    // Factory: Create appropriate cache policy implementation
    switch (policy) {
        case CachePolicy::LRU:
            policy_ = std::make_unique<LRUCache>(max_cache_size_mb);
            std::cout << "PageCache: Using LRU eviction policy (max: " 
                      << max_cache_size_mb << " MB)" << std::endl;
            break;
        case CachePolicy::LFU:
            policy_ = std::make_unique<LFUCache>(max_cache_size_mb);
            std::cout << "PageCache: Using LFU eviction policy (max: " 
                      << max_cache_size_mb << " MB)" << std::endl;
            break;
        default:
            // Fallback to LRU
            policy_ = std::make_unique<LRUCache>(max_cache_size_mb);
            std::cout << "PageCache: Using default LRU policy (max: " 
                      << max_cache_size_mb << " MB)" << std::endl;
            break;
    }
}

PageCache::~PageCache() {
    policy_->Clear();
}

bool PageCache::Get(uint64_t block_uuid, std::string& out_data) {
    return policy_->Get(block_uuid, out_data);
}

bool PageCache::Put(uint64_t block_uuid, const std::string& data) {
    return policy_->Put(block_uuid, data);
}

bool PageCache::Remove(uint64_t block_uuid) {
    return policy_->Remove(block_uuid);
}

bool PageCache::Contains(uint64_t block_uuid) {
    return policy_->Contains(block_uuid);
}

void PageCache::Clear() {
    policy_->Clear();
}

PageCachePolicy::CacheStats PageCache::GetStats() const {
    return policy_->GetStats();
}

void PageCache::ResetStats() {
    policy_->ResetStats();
}

std::string PageCache::GetPolicyName() const {
    return policy_->GetPolicyName();
}

}  // namespace fs_server
