#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include <mutex>

namespace fs_server {

/**
 * PageCache: In-memory page cache for block data
 * 
 * Responsibilities:
 * - Store frequently accessed blocks in memory
 * - Support cache hits for fast data retrieval
 * - Track cache statistics (hits, misses, evictions)
 * 
 * Thread-safety:
 * - Internally thread-safe using mutex
 * 
 * Usage:
 *   PageCache cache;
 *   if (!cache.Get(block_uuid, offset, length, out_data)) {
 *       // Cache miss - need to read from disk
 *   }
 *   cache.Put(block_uuid, data);
 */
class PageCache {
public:
    /**
     * Initialize the page cache
     */
    PageCache();
    ~PageCache();

    /**
     * Read block data from cache
     * 
     * @param block_uuid Unique identifier for the block
     * @param offset Starting byte offset (0-based)
     * @param length Number of bytes to read (0 = remaining bytes)
     * @param out_data [OUTPUT] Buffer to store read data
     * @return true if found in cache, false if cache miss
     */
    bool Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
             std::string& out_data);

    /**
     * Write block data to cache
     * 
     * Stores the entire block in the page cache for future access.
     * 
     * @param block_uuid Unique identifier for the block
     * @param data Block data to cache
     * @return true if successful, false if cache is full or other error
     */
    bool Put(uint64_t block_uuid, const std::string& data);

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
     */
    struct CacheStats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
        size_t current_size = 0;  // Current cache size in bytes
        size_t max_size = 0;      // Maximum cache size in bytes
    };

    CacheStats GetStats() const;
    void ResetStats();

private:
    // Cache storage: Maps block UUID to cached block data
    std::unordered_map<uint64_t, std::string> cache_map_;
    mutable std::mutex cache_mutex_;

    // Cache statistics
    mutable struct {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
    } stats_;
};

}  // namespace fs_server

