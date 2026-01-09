#pragma once

#include <string>
#include <cstdint>

namespace fs_server {

/**
 * DiskStore: Low-level disk I/O operations for block storage
 * 
 * BLOCK-ADDRESSABLE DESIGN:
 * - All operations work on WHOLE BLOCKS only
 * - No partial read/write support at this layer
 * - BlockStore handles partial operations by reading whole block,
 *   modifying in memory, and writing whole block back
 * 
 * Responsibilities:
 * - Handle disk read/write operations for entire blocks
 * - Manage file I/O (ofstream, ifstream)
 * - Support fsync for durability control
 * 
 * Thread-safety:
 * - NOT thread-safe internally; caller must synchronize access
 * - BlockStore handles all locking
 * 
 * Usage:
 *   DiskStore disk("/path/to/blocks/");
 *   disk.WriteBlock(block_uuid, data, sync=true);  // Write whole block
 *   std::string data = disk.ReadBlock(block_uuid); // Read whole block
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
     * Write entire block data to disk
     * 
     * @param block_uuid Unique identifier for the block
     * @param data Complete block data to write
     * @param sync If true, force fsync to disk for durability
     * @return true if successful, false if I/O error
     */
    bool WriteBlock(uint64_t block_uuid, const std::string& data, bool sync);

    /**
     * Read entire block data from disk
     * 
     * @param block_uuid Unique identifier for the block
     * @param out_data [OUTPUT] Buffer to store complete block data
     * @return true if successful, false if I/O error or block not found
     */
    bool ReadBlock(uint64_t block_uuid, std::string& out_data);

    /**
     * Delete a block file from disk
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
    bool BlockExists(uint64_t block_uuid);

    /**
     * Get size of a block on disk
     * 
     * @param block_uuid Block identifier
     * @return Block size in bytes, or 0 if block not found
     */
    uint32_t GetBlockSize(uint64_t block_uuid);

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

