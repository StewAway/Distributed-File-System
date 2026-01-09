#pragma once

#include <string>
#include <cstdint>
#include <memory>

namespace fs_server {

// Forward declarations
class PageCache;
class DiskStore;

/**
 * BlockStore: High-level abstraction managing both caching and disk I/O
 * 
 * Responsibilities:
 * - Manage page cache for frequently accessed blocks
 * - Delegate disk I/O to DiskStore
 * - Coordinate reads between cache and disk (cache-first approach)
 * - Coordinate writes to both cache and disk
 * - Provide partial read/write capabilities with offset support
 * 
 * Architecture:
 *   BlockStore (coordination layer)
 *       ├─> PageCache (in-memory cache)
 *       └─> DiskStore (disk I/O)
 * 
 * Thread-safety:
 * - Delegates to thread-safe components (PageCache, DiskStore)
 * 
 * Usage:
 *   BlockStore store("/path/to/blocks/");
 *   store.WriteBlock(block_uuid, data, sync=true);
 *   std::string data = store.ReadBlock(block_uuid, offset, length);
 */
class BlockStore {
public:
    /**
     * Initialize BlockStore with a blocks directory
     * @param blocks_dir Directory path for storing block files
     */
    explicit BlockStore(const std::string& blocks_dir, bool cache_enabled, uint64_t cache_size);
    ~BlockStore();

    /**
     * Write data to a block at specified offset
     * 
     * Supports partial writes using Read-Modify-Write internally:
     * - offset=0 with full block data: Write entire block
     * - offset=N: Write data starting at offset N
     * 
     * Cache and Disk are block-addressable (whole blocks only).
     * BlockStore handles partial operations by:
     * 1. Get whole block from cache/disk
     * 2. Modify the region [offset, offset+data.length()]
     * 3. Write whole block back
     * 
     * @param block_uuid Unique identifier for the block
     * @param offset Starting byte offset for write (0-based)
     * @param data Data to write at offset
     * @param sync If true, force fsync to disk for durability
     * @return true if successful, false if I/O error
     */
    bool WriteBlock(uint64_t block_uuid, uint32_t offset,
                    const std::string& data, bool sync);

    /**
     * Read data from a block at specified offset
     * 
     * Supports partial reads:
     * - offset=0, length=0: Read entire block
     * - offset=N, length=0: Read from offset N to end
     * - offset=N, length=M: Read M bytes starting at offset N
     * 
     * Cache and Disk are block-addressable (whole blocks only).
     * BlockStore handles partial reads by:
     * 1. Get whole block from cache (or disk on miss)
     * 2. Extract requested region [offset, offset+length]
     * 
     * @param block_uuid Unique identifier for the block
     * @param offset Starting byte offset (0-based)
     * @param length Number of bytes to read (0 = remaining bytes)
     * @param out_data [OUTPUT] Buffer to store read data
     * @return true if successful, false if I/O error or block not found
     */
    bool ReadBlock(uint64_t block_uuid, uint32_t offset, uint32_t length,
                   std::string& out_data);

    /**
     * Delete a block file from disk (and cache)
     * 
     * @param block_uuid Block identifier
     * @return true if successful, false if file not found or error
     */
    bool DeleteBlock(uint64_t block_uuid);

    /**
     * Check if a block file exists on disk
     * 
     * @param block_uuid Block identifier
     * @return true if block file exists, false otherwise
     */
    bool BlockFileExists(uint64_t block_uuid);

    /**
     * Get size of a block file on disk
     * 
     * @param block_uuid Block identifier
     * @return File size in bytes, or 0 if file not found
     */
    uint32_t GetBlockFileSize(uint64_t block_uuid);

    /**
     * Get access statistics for Tier 2 benchmarking
     * 
     * Returns aggregate stats about reads and writes from disk
     * Used to measure working set size, cache effectiveness
     */
    struct AccessStats {
        uint64_t total_reads = 0;
        uint64_t total_writes = 0;
        uint64_t total_bytes_read = 0;
        uint64_t total_bytes_written = 0;
    };
    
    AccessStats GetAccessStats() const;
    void ResetAccessStats();

private:
    std::unique_ptr<PageCache> cache_;   // In-memory cache
    bool cache_enabled_ = true;
    std::unique_ptr<DiskStore> disk_;    // Disk storage
    /**
     * 
     * @param block_uuid Block identifier
     * @return Full file path
     */
    std::string GetBlockPath(uint64_t block_uuid) const;
};

}  // namespace fs_server
