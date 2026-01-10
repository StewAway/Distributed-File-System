#include "fs_server/cache.hpp"
#include "fs_server/lru_cache.hpp"
#include "fs_server/lfu_cache.hpp"
#include <iostream>

namespace fs_server {

PageCache::PageCache(CachePolicy policy, uint64_t cache_size) {
    // Factory: Create appropriate cache policy implementation
    switch (policy) {
        case CachePolicy::LRU:
            policy_ = std::make_unique<LRUCache>(cache_size);
            std::cout << "PageCache: Using LRU eviction policy (max: " 
                      << cache_size << " Pages)" << std::endl;
            break;
        case CachePolicy::LFU:
            policy_ = std::make_unique<LFUCache>(cache_size);
            std::cout << "PageCache: Using LFU eviction policy (max: " 
                      << cache_size << " pages)" << std::endl;
            break;
        default:
            // Fallback to LRU
            policy_ = std::make_unique<LRUCache>(cache_size);
            std::cout << "PageCache: Using LRU eviction policy (max: " 
                      << cache_size << " Pages)" << std::endl;
            break;
    }
}

PageCache::~PageCache() {
    policy_->Clear();
}

bool PageCache::Get(uint64_t block_uuid, std::string& out_data) {
    return policy_->Get(block_uuid, out_data);
}

bool PageCache::Put(uint64_t block_uuid, const std::string& data, bool dirty) {
    return policy_->Put(block_uuid, data, dirty);
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

void PageCache::SetEvictionCallback(EvictionCallback callback) {
    policy_->SetEvictionCallback(std::move(callback));
}

void PageCache::FlushAll() {
    policy_->FlushAll();
}

}  // namespace fs_server
