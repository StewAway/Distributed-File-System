#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include <mutex>
#include <functional>

namespace fs_server {
constexpr uint64_t PAGE_SIZE = 64 * 1024;  // 64KB pages
constexpr uint64_t CACHE_SIZE = 256 * 1024 * 1024;  // 256MB default cache size
constexpr uint64_t MAX_CACHE_PAGES = CACHE_SIZE / PAGE_SIZE;

// Callback type for writing dirty pages on eviction
using EvictionCallback = std::function<void(uint64_t block_uuid, const std::string& data)>;

struct Page {
    std::string data;
    bool dirty = false;
    Page(const std::string& d, bool is_dirty = true) : data(d), dirty(is_dirty) {}
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
     * @param dirty If true, mark page as dirty (needs writeback on eviction)
     *              If false, page is clean (already synced with disk)
     * @return true if successful
     */
    virtual bool Put(uint64_t block_uuid, const std::string& data, bool dirty = true) = 0;

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

    /**
     * Set callback to be invoked when evicting dirty pages
     * 
     * @param callback Function to call with (block_uuid, data) when evicting dirty page
     */
    virtual void SetEvictionCallback(EvictionCallback callback) = 0;

    /**
     * Flush all dirty pages by invoking eviction callback for each
     * Used during shutdown to ensure data persistence
     */
    virtual void FlushAll() = 0;

    /**
     * Get the current number of dirty pages in the cache
     * @return Number of dirty pages
     */
    virtual uint64_t GetDirtyPageCount() const = 0;

    /**
     * Get the cache capacity (maximum number of pages)
     * @return Maximum number of pages the cache can hold
     */
    virtual uint64_t GetCapacity() const = 0;

    /**
     * Flush all dirty pages and return the count of pages flushed
     * After flushing, pages are marked clean but remain in cache
     * @return Number of dirty pages that were flushed
     */
    virtual uint64_t FlushDirtyPages() = 0;
};

}  // namespace fs_server
