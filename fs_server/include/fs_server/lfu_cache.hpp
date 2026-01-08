#pragma once

#include "page_cache_policy.hpp"
#include <unordered_map>
#include <map>
#include <list>
#include <mutex>

namespace fs_server {

/**
 * LFUCache: Least Frequently Used page cache implementation
 * 
 * Eviction Policy:
 * - Evicts the least frequently used block when cache is full
 * - Tracks access frequency for each cached block
 * - Uses recency as tiebreaker when blocks have equal frequency
 * - Suitable for workloads with varying access patterns
 * 
 * Data Structures:
 * - Frequency map: Maps access count to list of blocks with that frequency
 * - Hash map for O(1) lookups
 * 
 * Thread-safety:
 * - Internally thread-safe using mutex
 * 
 * Configuration:
 * - Default max cache size: 256MB (configurable via constructor)
 */
class LFUCache : public PageCachePolicy {
public:
    /**
     * Initialize LFU cache with specified max size
     * 
     * @param max_cache_size_mb Maximum cache size in megabytes (default: 256MB)
     */
    explicit LFUCache(size_t max_cache_size_mb = 256);
    ~LFUCache() override;

    bool Get(uint64_t block_uuid, std::string& out_data) override;

    bool Put(uint64_t block_uuid, const std::string& data) override;

    bool Remove(uint64_t block_uuid) override;

    bool Contains(uint64_t block_uuid) override;

    void Clear() override;

    CacheStats GetStats() const override;
    void ResetStats() override;

    std::string GetPolicyName() const override { return "LFU"; }

private:
    struct CacheEntry {
        std::string data;
        uint64_t frequency = 1;
        // Iterator to position in frequency list
    };

    size_t max_size_;  // Maximum cache size in bytes
    uint64_t min_frequency_ = 1;

    // Hash map: block_uuid -> (data, frequency, list_iterator)
    std::unordered_map<uint64_t, std::pair<std::string, uint64_t>> cache_map_;
    
    // Frequency map: frequency -> list of block_uuids with that frequency
    // List order represents insertion time (most recent at back)
    std::map<uint64_t, std::list<uint64_t>> frequency_map_;
    
    mutable std::mutex cache_mutex_;

    // Cache statistics
    mutable struct {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
    } stats_;

    /**
     * Evict the least frequently used block (with earliest insertion as tiebreaker)
     */
    void EvictLFU();

    /**
     * Update frequency of accessed block
     */
    void UpdateFrequency(uint64_t block_uuid);
};

}  // namespace fs_server
