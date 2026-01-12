#include "fs_server/block_store.hpp"
#include "fs_server/cache.hpp"
#include "fs_server/disk.hpp"
#include <sstream>
#include <iostream>
#include <algorithm>

namespace fs_server {

BlockStore::BlockStore(const std::string& blocks_dir, bool cache_enabled, uint64_t cache_size, CachePolicy cache_policy) {
    // Initialize disk first (needed for eviction callback)
    disk_ = std::make_unique<DiskStore>(blocks_dir);
    cache_enabled_ = cache_enabled;
    
    // Initialize cache only if enabled
    if (cache_enabled_) {
        cache_ = std::make_unique<PageCache>(cache_policy, cache_size);
        
        // Register eviction callback for write-back cache
        // When a dirty page is evicted, write the whole block to disk
        cache_->SetEvictionCallback([this](uint64_t block_uuid, const std::string& data) {
            std::cout << "BlockStore: Eviction callback - writing dirty block " 
                      << block_uuid << " to disk" << std::endl;
            if (!disk_->WriteBlock(block_uuid, data, true)) {
                std::cerr << "BlockStore: ERROR - failed to write evicted block " 
                          << block_uuid << " to disk" << std::endl;
            }
        });
        
        std::cout << "BlockStore: Initialized with write-back cache" << std::endl;
    } else {
        std::cout << "BlockStore: Initialized with cache DISABLED (disk-only mode)" << std::endl;
    }
}

BlockStore::~BlockStore() {
    // Flush all dirty pages before destruction (only if cache is enabled)
    if (cache_enabled_ && cache_) {
        std::cout << "BlockStore: Flushing all dirty pages before destruction" << std::endl;
        cache_->FlushAll();
    }
    std::cout << "BlockStore: Destroyed" << std::endl;
}

std::string BlockStore::GetBlockPath(uint64_t block_uuid) const {
    std::stringstream ss;
    ss << "/blk_" << block_uuid << ".img";
    return ss.str();
}

bool BlockStore::WriteBlock(uint64_t block_uuid, uint64_t offset,
                            const std::string& data, bool sync) {
    try {
        // Disk-only mode: bypass cache entirely
        if (!cache_enabled_) {
            std::string block_data;
            bool block_exists_on_disk = disk_->BlockExists(block_uuid);
            
            // Get existing block data if needed for partial write
            if (offset > 0 && block_exists_on_disk) {
                std::cout << "BlockStore: [disk-only] Reading existing block " << block_uuid 
                          << " from disk" << std::endl;
                if (!disk_->ReadBlock(block_uuid, block_data)) {
                    std::cerr << "BlockStore: Failed to read block " << block_uuid 
                              << " from disk" << std::endl;
                    return false;
                }
            }
            
            // Modify the block in memory
            size_t required_size = offset + data.length();
            if (block_data.length() < required_size) {
                block_data.resize(required_size, '\0');
            }
            std::copy(data.begin(), data.end(), block_data.begin() + offset);
            
            std::cout << "BlockStore: [disk-only] Write block " << block_uuid
                      << " at offset " << offset << ", " << data.length() << " bytes" << std::endl;
            
            // Write directly to disk
            if (!disk_->WriteBlock(block_uuid, block_data, false)) {
                std::cerr << "BlockStore: Failed to write block " << block_uuid 
                          << " to disk" << std::endl;
                return false;
            }
            return true;
        }
        
        // Cache-enabled mode: use write-back cache strategy
        std::string block_data;
        bool block_exists_in_cache = cache_->Get(block_uuid, block_data);
        bool block_exists_on_disk = !block_exists_in_cache && disk_->BlockExists(block_uuid);
        
        // Step 1: Get existing block data if needed for partial write
        if (offset > 0 || block_exists_in_cache || block_exists_on_disk) {
            if (block_exists_in_cache) {
                std::cout << "BlockStore: Cache hit for block " << block_uuid << std::endl;
                // block_data already populated from cache_->Get()
            } else if (block_exists_on_disk) {
                // Cache miss - read whole block from disk
                std::cout << "BlockStore: Cache miss - reading block " << block_uuid 
                          << " from disk" << std::endl;
                if (!disk_->ReadBlock(block_uuid, block_data)) {
                    std::cerr << "BlockStore: Failed to read block " << block_uuid 
                              << " from disk" << std::endl;
                    return false;
                }
            }
        }
        
        // Step 2: Modify the block in memory
        // Ensure block is large enough for the write
        size_t required_size = offset + data.length();
        if (block_data.length() < required_size) {
            block_data.resize(required_size, '\0');
        }
        
        // Copy data into block at offset
        std::copy(data.begin(), data.end(), block_data.begin() + offset);
        
        std::cout << "BlockStore: Write block " << block_uuid
                  << " at offset " << offset << ", " << data.length() << " bytes" << std::endl;
        
        // Step 3: Write whole block using write-back cache strategy
        if (block_exists_in_cache) {
            // Update cache and mark dirty for later write-back
            cache_->Put(block_uuid, block_data, true);
        } else {
            // New block or only on disk: write-back strategy — cache as dirty, no immediate disk write
            cache_->Put(block_uuid, block_data, true);
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception writing block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::ReadBlock(uint64_t block_uuid, uint64_t offset, uint64_t length,
                           std::string& out_data) {
    try {
        std::string block_data;
        
        // Disk-only mode: bypass cache entirely
        if (!cache_enabled_) {
            std::cout << "BlockStore: [disk-only] Reading block " << block_uuid 
                      << " from disk" << std::endl;
            
            if (!disk_->ReadBlock(block_uuid, block_data)) {
                std::cerr << "BlockStore: Failed to read block " << block_uuid 
                          << " from disk" << std::endl;
                return false;
            }
        } else {
            // Cache-enabled mode: check cache first
            // Step 1: Get whole block from cache or disk
            if (cache_->Get(block_uuid, block_data)) {
                std::cout << "BlockStore: Cache hit for block " << block_uuid << std::endl;
            } else {
                // Cache miss - read whole block from disk
                std::cout << "BlockStore: Cache miss for block " << block_uuid 
                          << " - reading from disk" << std::endl;
                
                if (!disk_->ReadBlock(block_uuid, block_data)) {
                    std::cerr << "BlockStore: Failed to read block " << block_uuid 
                              << " from disk" << std::endl;
                    return false;
                }
                
                // Cache the whole block (clean, since it's from disk)
                cache_->Put(block_uuid, block_data, false);
            }
        }
        
        // Step 2: Extract the requested portion
        uint64_t block_size = block_data.length();
        
        if (offset >= block_size) {
            // Offset beyond block - return empty
            out_data.clear();
            return true;
        }
        
        // Calculate actual length to return
        uint64_t actual_length = length;
        if (length == 0) {
            // Return from offset to end
            actual_length = block_size - offset;
        } else {
            // Clamp to available data
            actual_length = std::min(length, block_size - offset);
        }
        
        // Extract substring
        out_data = block_data.substr(offset, actual_length);
        
        std::cout << "BlockStore: Read " << actual_length << " bytes from block " 
                  << block_uuid << " (offset=" << offset << ")" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception reading block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::DeleteBlock(uint64_t block_uuid) {
    try {
        // Remove from cache (only if cache is enabled)
        if (cache_enabled_ && cache_) {
            cache_->Remove(block_uuid);
        }
        
        // Delete from disk
        if (!disk_->DeleteBlock(block_uuid)) {
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
    return disk_->BlockExists(block_uuid);
}

uint64_t BlockStore::GetBlockFileSize(uint64_t block_uuid) {
    // First check cache (only if enabled)
    if (cache_enabled_ && cache_) {
        std::string cached_data;
        if (cache_->Get(block_uuid, cached_data)) {
            return cached_data.length();
        }
    }
    // Fallback to disk
    return disk_->GetBlockSize(block_uuid);
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

uint64_t BlockStore::GetDirtyPageCount() const {
    if (!cache_enabled_ || !cache_) {
        return 0;
    }
    return cache_->GetDirtyPageCount();
}

uint64_t BlockStore::GetCacheCapacity() const {
    if (!cache_enabled_ || !cache_) {
        return 0;
    }
    return cache_->GetCapacity();
}

uint64_t BlockStore::FlushDirtyPages() {
    if (!cache_enabled_ || !cache_) {
        return 0;
    }
    return cache_->FlushDirtyPages();
}

}  // namespace fs_server
