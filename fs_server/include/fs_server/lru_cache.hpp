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
    explicit LRUCache(size_t max_cache_pages = MAX_CACHE_PAGES);
    ~LRUCache() override;

    bool Get(uint64_t block_uuid, std::string& out_data) override;

    bool Put(uint64_t block_uuid, const std::string& data) override;

    bool Remove(uint64_t block_uuid) override;

    bool Contains(uint64_t block_uuid) override;

    void Clear() override;

    CacheStats GetStats() const override;
    void ResetStats() override;

    std::string GetPolicyName() const override { return "LRU"; }

private:
    struct LinkedListNode {
        uint64_t block_uuid;
        Page page;
        LinkedListNode* prev;
        LinkedListNode* next;
        
        LinkedListNode() : block_uuid(0), page(""), prev(nullptr), next(nullptr) {}
        LinkedListNode(uint64_t uuid, const std::string& d) 
            : block_uuid(uuid), page(d), prev(nullptr), next(nullptr) {}
    };

    int capacity_;  // Number of blocks that can be cached
    int size_;  // Current number of blocks in cache
    
    LinkedListNode* head_;  // Sentinel head node
    LinkedListNode* tail_;  // Sentinel tail node
    
    // Hash map for O(1) lookups: block_uuid -> LinkedListNode*
    std::unordered_map<uint64_t, LinkedListNode*> cache_map_;
    
    mutable std::mutex cache_mutex_;

    // Cache statistics
    mutable struct {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
    } stats_;

    /**
     * Remove a node from the doubly-linked list
     */
    void RemoveNode(LinkedListNode* node);
    
    /**
     * Add a node right after head (most recently used position)
     */
    void AddNode(LinkedListNode* node);
    
    /**
     * Evict the least recently used (tail->prev) block from cache
     */
    void EvictLRU();
};

}  // namespace fs_server
