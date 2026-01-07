#pragma once

#include <string>
#include <cstdint>

namespace fs_server {

/**
 * DiskStore: Low-level disk I/O operations for block storage
 * 
 * Responsibilities:
 * - Handle disk read/write operations
 * - Manage file I/O (ofstream, ifstream)
 * - Support fsync for durability control
 * - Provide partial read/write capabilities with offset support
 * 
 * Purpose of this abstraction:
 * - Isolate disk I/O logic from BlockStore
 * - Separate disk operations from caching logic
 * - Keep filesystem implementation details (seekg, fsync, etc) localized
 * 
 * Thread-safety:
 * - NOT thread-safe internally; caller must synchronize access
 * - BlockStore handles all locking
 * 
 * Usage:
 *   DiskStore disk("/path/to/blocks/");
 *   disk.WriteBlockToDisk(block_uuid, data, sync=true);
 *   std::string data = disk.ReadBlockFromDisk(block_uuid, offset, length);
 */
class DiskStore {
public:
    /**
     * Initialize DiskStore with a blocks directory
     * @param blocks_dir Directory path for storing block files
     */
    explicit DiskStore(const std::string& blocks_dir);
    ~DiskStore();

    /**
     * Write block data to disk
     * 
     * @param block_uuid Unique identifier for the block
     * @param data Data to write to disk
     * @param sync If true, force fsync to disk for durability.
     *             If false, data stays in kernel page cache (faster but risky).
     * @return true if successful, false if I/O error
     * 
     * Data flow:
     *   Application buffer (data)
     *       ↓
     *   C++ ofstream buffer
     *       ↓
     *   file.flush() → OS page cache
     *       ↓
     *   fsync() [if sync=true] → Physical disk
     */
    bool WriteBlockToDisk(uint64_t block_uuid, const std::string& data, bool sync);

    /**
     * Read block data from disk
     * 
     * Supports partial reads for efficient data retrieval:
     * - offset=0, length=0: Read entire block
     * - offset=N, length=0: Read from offset N to end
     * - offset=N, length=M: Read M bytes starting at offset N
     * 
     * @param block_uuid Unique identifier for the block
     * @param offset Starting byte offset (0-based)
     * @param length Number of bytes to read (0 = remaining bytes)
     * @param out_data [OUTPUT] Buffer to store read data
     * @return true if successful, false if I/O error or block not found
     * 
     * Data flow:
     *   Physical disk
     *       ↓
     *   OS page cache [if cached]
     *       ↓
     *   C++ ifstream buffer
     *       ↓
     *   Application buffer (out_data)
     */
    bool ReadBlockFromDisk(uint64_t block_uuid, uint32_t offset, uint32_t length,
                           std::string& out_data);

    /**
     * Delete a block file from disk
     * 
     * @param block_uuid Block identifier
     * @return true if successful, false if file not found or error
     */
    bool DeleteBlockFromDisk(uint64_t block_uuid);

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
     * Returns aggregate stats about reads and writes
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
    std::string blocks_dir_;
    
    // Tier 2: Access statistics for profiling
    mutable struct {
        uint64_t total_reads = 0;
        uint64_t total_writes = 0;
        uint64_t total_bytes_read = 0;
        uint64_t total_bytes_written = 0;
    } stats_;

    /**
     * Convert block UUID to full file path
     * Format: blocks_dir_/blk_<uuid>.img
     * 
     * @param block_uuid Block identifier
     * @return Full file path
     */
    std::string GetBlockPath(uint64_t block_uuid) const;
};

}  // namespace fs_server

