#pragma once

#include "page_cache_policy.hpp"
#include <list>
#include <unordered_map>
#include <mutex>

namespace fs_server {

/**
 * LRUCache: Least Recently Used page cache implementation
 * 
 * Eviction Policy:
 * - Evicts the least recently used block when cache is full
 * - Updates access order on every Get or Put operation
 * - Suitable for workloads with temporal locality
 * 
 * Data Structures:
 * - Doubly-linked list for O(1) removal and reordering
 * - Hash map for O(1) lookups
 * 
 * Thread-safety:
 * - Internally thread-safe using mutex
 * 
 * Configuration:
 * - Default max cache size: 256MB (configurable via constructor)
 */
class LRUCache : public PageCachePolicy {
public:
    /**
     * Initialize LRU cache with specified max size
     * 
     * @param max_cache_size_mb Maximum cache size in megabytes (default: 256MB)
     */
    explicit LRUCache(size_t max_cache_size_mb = 256);
    ~LRUCache() override;

    bool Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
             std::string& out_data) override;

    bool Put(uint64_t block_uuid, const std::string& data) override;

    bool Remove(uint64_t block_uuid) override;

    bool Contains(uint64_t block_uuid) override;

    void Clear() override;

    CacheStats GetStats() const override;
    void ResetStats() override;

    std::string GetPolicyName() const override { return "LRU"; }

private:
    struct CacheEntry {
        uint64_t block_uuid;
        std::string data;
        // Iterator to position in the access order list
        // Will be set by LRUCache implementation
    };

    size_t max_size_;  // Maximum cache size in bytes

    // Double-linked list to track access order (most recent at back)
    std::list<uint64_t> access_order_;
    
    // Hash map for O(1) lookups: block_uuid -> (data, list_iterator)
    std::unordered_map<uint64_t, std::pair<std::string, std::list<uint64_t>::iterator>> cache_map_;
    
    mutable std::mutex cache_mutex_;

    // Cache statistics
    mutable struct {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
    } stats_;

    /**
     * Evict the least recently used (front) block from cache
     */
    void EvictLRU();
};

}  // namespace fs_server
