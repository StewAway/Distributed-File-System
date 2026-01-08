#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include <mutex>

namespace fs_server {
constexpr uint64_t PAGE_SIZE = 64 * 1024;  // 64KB pages
constexpr uint64_t CACHE_SIZE = 256 * 1024 * 1024;  // 256MB default cache size
constexpr uint64_t MAX_CACHE_PAGES = CACHE_SIZE / PAGE_SIZE;
struct Page {
    std::string data;
    bool dirty = false;
    Page(const std::string& d) : data(d) {}
};


/**
 * PageCachePolicy: Abstract base class for cache eviction policies
 * 
 * Defines the interface for different cache replacement strategies (LRU, LFU, etc.)
 * Concrete implementations should override virtual methods to implement specific policies.
 * 
 * Thread-safety:
 * - Derived classes should be internally thread-safe
 */
class PageCachePolicy {
public:
    virtual ~PageCachePolicy() = default;

    /**
     * Read block data from cache
     * 
     * @param block_uuid Unique identifier for the block
     * @param offset Starting byte offset (0-based)
     * @param length Number of bytes to read (0 = remaining bytes)
     * @param out_data [OUTPUT] Buffer to store read data
     * @return true if found in cache (cache hit), false if cache miss
     */
    virtual bool Get(uint64_t block_uuid, std::string& out_data) = 0;

    /**
     * Write block data to cache
     * 
     * @param block_uuid Unique identifier for the block
     * @param data Block data to cache
     * @return true if successful
     */
    virtual bool Put(uint64_t block_uuid, const std::string& data) = 0;

    /**
     * Remove a block from cache
     * 
     * @param block_uuid Block identifier
     * @return true if block was in cache and removed
     */
    virtual bool Remove(uint64_t block_uuid) = 0;

    /**
     * Check if a block is in cache
     * 
     * @param block_uuid Block identifier
     * @return true if block exists in cache
     */
    virtual bool Contains(uint64_t block_uuid) = 0;

    /**
     * Clear all cached blocks
     */
    virtual void Clear() = 0;

    /**
     * Get cache statistics
     */
    struct CacheStats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
        size_t current_size = 0;  // Current cache size in bytes
        size_t max_size = 0;      // Maximum cache size in bytes
        std::string policy_name;  // Name of the eviction policy (e.g., "LRU", "LFU")
    };

    virtual CacheStats GetStats() const = 0;
    virtual void ResetStats() = 0;

    /**
     * Get the name of this cache policy
     * 
     * @return Policy name (e.g., "LRU", "LFU")
     */
    virtual std::string GetPolicyName() const = 0;
};

}  // namespace fs_server
