#pragma once

#include "page_cache_policy.hpp"
#include <unordered_map>
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
 * - Frequency map: Maps access count to doubly-linked list of blocks with that frequency
 * - Hash map for O(1) lookups
 * - Doubly-linked list per frequency for O(1) eviction
 * 
 * Thread-safety:
 * - Internally thread-safe using mutex
 * 
 * Configuration:
 * - Cache size specified in number of pages
 */

// Forward declaration
struct LFUNode;
class FrequencyList;

/**
 * LFUNode: Node in the frequency-based doubly-linked list
 */
struct LFUNode {
    uint64_t block_uuid;
    Page page;
    uint64_t freq;
    LFUNode* prev;
    LFUNode* next;

    LFUNode() : block_uuid(0), page("", false), freq(1), prev(nullptr), next(nullptr) {}
    LFUNode(uint64_t uuid, const std::string& data, bool dirty = true) 
        : block_uuid(uuid), page(data, dirty), freq(1), prev(nullptr), next(nullptr) {}
};

/**
 * FrequencyList: Doubly-linked list for nodes with the same frequency
 * - Head is most recently used, Tail is least recently used
 * - Eviction happens from tail (LRU within same frequency)
 */
class FrequencyList {
public:
    LFUNode* head;
    LFUNode* tail;
    size_t size;

    FrequencyList() : size(0) {
        head = new LFUNode();
        tail = new LFUNode();
        head->next = tail;
        tail->prev = head;
    }

    ~FrequencyList() {
        // Only delete sentinel nodes; actual nodes are managed by LFUCache
        delete head;
        delete tail;
    }

    void remove(LFUNode* node) {
        if (node == nullptr || size == 0) return;
        node->prev->next = node->next;
        node->next->prev = node->prev;
        --size;
    }

    void add(LFUNode* node) {
        if (node == nullptr) return;
        // Add to front (most recently used within this frequency)
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
        node->prev = head;
        ++size;
    }

    LFUNode* getTail() {
        if (size == 0) return nullptr;
        return tail->prev;
    }

    bool isEmpty() const {
        return size == 0;
    }
};

class LFUCache : public PageCachePolicy {
public:
    /**
     * Initialize LFU cache with specified max size
     * 
     * @param cache_size Number of pages that can be cached
     */
    explicit LFUCache(size_t cache_size);
    ~LFUCache() override;

    bool Get(uint64_t block_uuid, std::string& out_data) override;

    bool Put(uint64_t block_uuid, const std::string& data, bool dirty = true) override;

    bool Remove(uint64_t block_uuid) override;

    bool Contains(uint64_t block_uuid) override;

    void Clear() override;

    CacheStats GetStats() const override;
    void ResetStats() override;

    std::string GetPolicyName() const override { return "LFU"; }

    void SetEvictionCallback(EvictionCallback callback) override;
    void FlushAll() override;

private:
    size_t capacity_;   // Maximum number of pages in cache
    size_t size_;       // Current number of pages in cache
    uint64_t min_freq_; // Minimum frequency (for O(1) eviction)

    // Hash map: block_uuid -> LFUNode*
    std::unordered_map<uint64_t, LFUNode*> cache_map_;
    
    // Frequency map: frequency -> FrequencyList*
    std::unordered_map<uint64_t, FrequencyList*> freq_map_;
    
    mutable std::mutex cache_mutex_;

    // Eviction callback for write-back
    EvictionCallback eviction_callback_;

    // Cache statistics
    mutable struct {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
    } stats_;

    /**
     * Evict the least frequently used block (with LRU tiebreaker)
     */
    void EvictLFU();

    /**
     * Helper to get or create a frequency list
     */
    FrequencyList* getOrCreateFreqList(uint64_t freq);
};

}  // namespace fs_server
