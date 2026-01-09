#pragma once

#include <string>
#include <cstdint>
#include <memory>
#include "page_cache_policy.hpp"

namespace fs_server {

// Enum for cache policy selection
enum class CachePolicy {
    LRU,  // Least Recently Used
    LFU   // Least Frequently Used
};

/**
 * PageCache: Wrapper for page cache implementation with pluggable policies
 * 
 * Responsibilities:
 * - Store frequently accessed blocks in memory
 * - Support cache hits for fast data retrieval
 * - Track cache statistics (hits, misses, evictions)
 * - Allow switching between different eviction policies (LRU, LFU)
 * 
 * Thread-safety:
 * - Delegates to underlying policy implementation
 * 
 * Usage:
 *   // Create cache with LRU policy
 *   PageCache cache(CachePolicy::LRU);
 *   
 *   if (!cache.Get(block_uuid, offset, length, out_data)) {
 *       // Cache miss - need to read from disk
 *   }
 *   cache.Put(block_uuid, data);
 *   
 *   // Get statistics
 *   auto stats = cache.GetStats();
 *   std::cout << "Hits: " << stats.hits << ", Misses: " << stats.misses << std::endl;
 */
class PageCache {
public:
    /**
     * Initialize the page cache with specified policy
     * 
     * @param policy The eviction policy to use (LRU, LFU)
     * @param max_cache_size_mb Maximum cache size in megabytes (default: 256MB)
     */
    PageCache(CachePolicy policy = CachePolicy::LRU, uint64_t cache_size);
    ~PageCache();

    /**
     * Read block data from cache
     * 
     * @param block_uuid Unique identifier for the block
     * @param out_data [OUTPUT] Buffer to store read data
     * @return true if found in cache, false if cache miss
     */
    bool Get(uint64_t block_uuid, std::string& out_data);

    /**
     * Write block data to cache
     * 
     * Stores the entire block in the page cache for future access.
     * 
     * @param block_uuid Unique identifier for the block
     * @param data Block data to cache
     * @param dirty If true, mark as dirty (needs writeback). If false, clean.
     * @return true if successful, false if cache is full or other error
     */
    bool Put(uint64_t block_uuid, const std::string& data, bool dirty = true);

    /**
     * Remove a block from cache
     * 
     * @param block_uuid Block identifier
     * @return true if block was in cache and removed, false otherwise
     */
    bool Remove(uint64_t block_uuid);

    /**
     * Check if a block is in cache
     * 
     * @param block_uuid Block identifier
     * @return true if block exists in cache, false otherwise
     */
    bool Contains(uint64_t block_uuid);

    /**
     * Clear all cached blocks
     */
    void Clear();

    /**
     * Get cache statistics
     * 
     * @return Statistics including hits, misses, evictions, and sizes
     */
    PageCachePolicy::CacheStats GetStats() const;
    
    /**
     * Reset cache statistics
     */
    void ResetStats();

    /**
     * Get the name of the current cache policy
     * 
     * @return Policy name (e.g., "LRU", "LFU")
     */
    std::string GetPolicyName() const;

    /**
     * Set callback to be invoked when evicting dirty pages
     * 
     * @param callback Function to call with (block_uuid, data) when evicting dirty page
     */
    void SetEvictionCallback(EvictionCallback callback);

    /**
     * Flush all dirty pages by invoking eviction callback for each
     * Used during shutdown to ensure data persistence
     */
    void FlushAll();

private:
    std::unique_ptr<PageCachePolicy> policy_;
};

}  // namespace fs_server
