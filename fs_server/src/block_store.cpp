#include "fs_server/block_store.hpp"
#include "fs_server/cache.hpp"
#include "fs_server/disk.hpp"
#include <sstream>
#include <iostream>

namespace fs_server {

BlockStore::BlockStore(const std::string& blocks_dir) {
    // Initialize cache and disk components
    cache_ = std::make_unique<PageCache>();
    disk_ = std::make_unique<DiskStore>(blocks_dir);
    
    std::cout << "BlockStore: Initialized with cache and disk layers" << std::endl;
}

BlockStore::~BlockStore() {
    // Cleanup on destruction
    std::cout << "BlockStore: Destroyed" << std::endl;
}

std::string BlockStore::GetBlockPath(uint64_t block_uuid) const {
    std::stringstream ss;
    ss << "/blk_" << block_uuid << ".img";
    return ss.str();
}

bool BlockStore::WriteBlock(uint64_t block_uuid, const std::string& data, bool sync) {
    try {
        // Write to disk first
        if (!disk_->WriteBlockToDisk(block_uuid, data, sync)) {
            std::cerr << "BlockStore: Failed to write block " << block_uuid 
                      << " to disk" << std::endl;
            return false;
        }
        
        // Update cache with written data
        if (!cache_->Put(block_uuid, data)) {
            std::cerr << "BlockStore: Warning - failed to update cache for block " 
                      << block_uuid << std::endl;
            // Not a critical error - disk write succeeded
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception writing block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::ReadBlock(uint64_t block_uuid, uint32_t offset, uint32_t length,
                           std::string& out_data) {
    try {
        // Try cache first (fast path)
        if (cache_->Get(block_uuid, offset, length, out_data)) {
            std::cout << "BlockStore: Cache hit for block " << block_uuid << std::endl;
            return true;
        }
        
        // Cache miss - read from disk
        std::cout << "BlockStore: Cache miss for block " << block_uuid 
                  << " - reading from disk" << std::endl;
        
        if (!disk_->ReadBlockFromDisk(block_uuid, offset, length, out_data)) {
            std::cerr << "BlockStore: Failed to read block " << block_uuid 
                      << " from disk" << std::endl;
            return false;
        }
        
        // Try to update cache with entire block data (for future cache hits)
        if (offset == 0 && length == 0) {
            // We read entire block, cache it
            if (!cache_->Put(block_uuid, out_data)) {
                std::cerr << "BlockStore: Warning - failed to cache block " 
                          << block_uuid << std::endl;
                // Not a critical error - read succeeded
            }
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception reading block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::DeleteBlock(uint64_t block_uuid) {
    try {
        // Remove from cache
        cache_->Remove(block_uuid);
        
        // Delete from disk
        if (!disk_->DeleteBlockFromDisk(block_uuid)) {
            std::cerr << "BlockStore: Failed to delete block " << block_uuid 
                      << " from disk" << std::endl;
            return false;
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception deleting block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::BlockFileExists(uint64_t block_uuid) {
    return disk_->BlockFileExists(block_uuid);
}

uint32_t BlockStore::GetBlockFileSize(uint64_t block_uuid) {
    return disk_->GetBlockFileSize(block_uuid);
}

BlockStore::AccessStats BlockStore::GetAccessStats() const {
    auto disk_stats = disk_->GetAccessStats();
    return {
        disk_stats.total_reads,
        disk_stats.total_writes,
        disk_stats.total_bytes_read,
        disk_stats.total_bytes_written
    };
}

void BlockStore::ResetAccessStats() {
    disk_->ResetAccessStats();
}

}  // namespace fs_server
