#pragma once

#include "fs_service/fs.grpc.pb.h"
#include "fs_server/block_store.hpp"
#include <fs_server/page_cache_policy.hpp>
#include <memory>
#include <unordered_map>
#include <string>
#include <vector>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include <cstring>

namespace fs_server {

// ============================================================================
// Block Constants
// ============================================================================
constexpr uint64_t BLOCK_SIZE = 65536;  // 64 KB blocks (configurable)
constexpr uint64_t MAX_BLOCKS = 1000000;  // Maximum blocks per datanode

/**
 * BlockMetadata: Stores metadata about a block
 * 
 * Usage: Track block properties for integrity checking and logging
 * Future: Add cache hit stats, access patterns for page cache optimization
 */
struct BlockMetadata {
    uint64_t block_uuid;
    uint64_t size;              // Current size (may be < BLOCK_SIZE if partial)
    std::string created_at;     // ISO 8601 timestamp
    std::string checksum;       // SHA256 or similar (for integrity)
    uint64_t access_count = 0;  // For future cache optimization
    
    BlockMetadata() : block_uuid(0), size(0) {}
    BlockMetadata(uint64_t uuid, uint64_t sz, const std::string& ts, const std::string& cksum)
        : block_uuid(uuid), size(sz), created_at(ts), checksum(cksum) {}
};

/**
 * BlockManager: Manages block metadata and delegates disk I/O to BlockStore
 * 
 * Responsibilities:
 * - Manage block metadata (size, checksum, timestamps, access patterns)
 * - Track block inventory in memory
 * - Delegate disk read/write operations to BlockStore
 * - Prepare data structures for future caching layers
 * 
 * Architecture:
 *   BlockManager (metadata + logic)
 *       ↓
 *   BlockStore (disk I/O abstraction)
 *       ↓
 *   OS filesystem
 * 
 * Thread-safe: Uses mutex to protect concurrent access to blocks_map_
 * 
 * Future layers:
 * - Page cache will sit between BlockManager and BlockStore
 * - BlockManager will query cache before delegating to BlockStore
 * 
 * Usage:
 *   BlockManager manager("/path/to/blocks/");
 *   manager.WriteBlock(block_uuid, data);
 *   auto data = manager.ReadBlock(block_uuid, offset, length);
 */
class BlockManager {
public:
    /**
     * Initialize BlockManager with a blocks directory path
     * Creates the directory if it doesn't exist
     * @param blocks_dir Directory for storing block files
     * @param cache_enabled Enable or disable cache (default: false)
     * @param cache_size Size of the cache in pages (default: 4096)
     */
    explicit BlockManager(const std::string& blocks_dir, bool cache_enabled, uint64_t cache_size);
    ~BlockManager();

    /**
     * Write a block to disk
     * 
     * @param block_uuid Unique identifier for the block
     * @param data Data to write
     * @param offset Offset within the block (for page cache optimization)
     * @param sync Force fsync if true
     * @return true if successful, false otherwise
     * 
     * Future: Support partial writes with offset for page cache
     */
    bool WriteBlock(uint64_t block_uuid, const std::string& data, 
                    uint64_t offset = 0, bool sync = true);

    /**
     * Read a block from disk
     * 
     * @param block_uuid Unique identifier for the block
     * @param offset Offset within the block (for page cache optimization)
     * @param length Number of bytes to read (0 = entire block)
     * @param out_data Output data (filled by function)
     * @return true if successful, false otherwise
     * 
     * Future: Support partial reads with offset for page cache
     */
    bool ReadBlock(uint64_t block_uuid, uint64_t offset, uint64_t length,
                   std::string& out_data);

    /**
     * Delete a block from disk
     * 
     * @param block_uuid Block to delete
     * @return true if successful, false otherwise
     */
    bool DeleteBlock(uint64_t block_uuid);

    /**
     * Check if a block exists
     */
    bool BlockExists(uint64_t block_uuid);

    /**
     * Get metadata for a block
     */
    bool GetBlockMetadata(uint64_t block_uuid, BlockMetadata& out_metadata);

    /**
     * Get list of all blocks stored on this node
     * Used for heartbeat reports to master
     */
    std::vector<uint64_t> GetAllBlocks();

    /**
     * Get total storage used (in bytes)
     */
    uint64_t GetTotalStorageUsed();

private:
    std::string blocks_dir_;
    bool cache_enabled_;
    uint64_t cache_size_;
    std::unordered_map<uint64_t, BlockMetadata> blocks_map_;
    mutable std::mutex blocks_mutex_;
    std::unique_ptr<BlockStore> block_store_;  // Handles disk I/O

    /**
     * Convert block UUID to filename
     * Format: blocks/blk_<uuid>.img
     */
    std::string GetBlockPath(uint64_t block_uuid) const;

    /**
     * Calculate SHA256 checksum of data
     */
    std::string CalculateChecksum(const std::string& data);

    /**
     * Get current timestamp in ISO 8601 format
     */
    std::string GetCurrentTimestamp();

    /**
     * Load existing blocks from disk on initialization
     */
    void LoadExistingBlocks();
};

/**
 * FSServerServiceImpl: gRPC service implementation for data node
 * 
 * This service handles all block storage operations:
 * - ReadBlock: Read block data (with offset support for page cache)
 * - WriteBlock: Write block data (with fsync option)
 * - DeleteBlock: Remove a block
 * - GetBlockInfo: Get metadata about a block
 * - HeartBeat: Report to master (blocks stored, health status)
 * 
 * Thread-safe: Delegates to thread-safe BlockManager
 * 
 * Usage:
 *   auto impl = std::make_unique<FSServerServiceImpl>(datanode_id, blocks_dir);
 *   grpc::ServerBuilder builder;
 *   builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
 *   builder.RegisterService(impl.get());
 *   auto server = builder.BuildAndStart();
 */
class FSServerServiceImpl final : public FSServerService::Service {
public:
    /**
     * Initialize the datanode service
     * 
     * @param datanode_id Unique identifier for this datanode
     * @param blocks_dir Directory to store block files
     * @param cache_enabled Enable or disable cache (default: false)
     * @param cache_size Size of the cache in pages (default: 4096 pages)
     */
    FSServerServiceImpl(const std::string& datanode_id, const std::string& blocks_dir, bool cache_enabled, uint64_t cache_size);

    /**
     * ReadBlockDataServer RPC: Read data from a block
     * 
     * Supports partial reads with offset (for future page cache optimization):
     * - offset = 0, length = 0: Read entire block
     * - offset = N, length = 0: Read from offset N to end
     * - offset = N, length = M: Read M bytes starting at offset N
     */
    grpc::Status ReadBlockDataServer(grpc::ServerContext* context,
                                     const ReadBlockRequest* request,
                                     ReadBlockResponse* response) override;

    /**
     * WriteBlockDataServer RPC: Write data to a block
     * 
     * Creates or overwrites the block with provided data.
     * Supports full block writes (offset support for future page cache).
     * 
     * If sync=true, forces fsync to disk after write.
     * This ensures durability but is slower than async writes.
     */
    grpc::Status WriteBlockDataServer(grpc::ServerContext* context,
                                      const WriteBlockRequest* request,
                                      StatusResponse* response) override;

    /**
     * DeleteBlockDataServer RPC: Delete a block
     * 
     * Removes the block file from disk.
     * Can be called by master to clean up unused blocks.
     */
    grpc::Status DeleteBlockDataServer(grpc::ServerContext* context,
                                       const DeleteBlockRequest* request,
                                       StatusResponse* response) override;

    /**
     * GetBlockInfoDataServer RPC: Get metadata about a block
     * 
     * Returns: size, creation timestamp, checksum
     * Used by master to verify block integrity
     */
    grpc::Status GetBlockInfoDataServer(grpc::ServerContext* context,
                                        const GetBlockInfoRequest* request,
                                        GetBlockInfoResponse* response) override;

    /**
     * HeartBeatDataServer RPC: Health check and block inventory report
     * 
     * Called periodically by master to:
     * - Verify datanode is alive
     * - Report all blocks stored on this node
     * - Receive commands from master (for future use)
     */
    grpc::Status HeartBeatDataServer(grpc::ServerContext* context,
                                     const HeartBeatRequest* request,
                                     HeartBeatResponse* response) override;

    /**
     * Get statistics about this datanode
     */
    std::string GetStatistics();

private:
    std::string datanode_id_;
    std::unique_ptr<BlockManager> block_manager_;
    uint64_t request_count_ = 0;
    std::mutex stats_mutex_;
};

}  // namespace fs_server
