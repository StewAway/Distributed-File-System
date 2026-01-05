#include "fs_master/user_context.hpp"
#include <cstdint>
#include <mutex>

namespace fs_master {

// ============================================================================
// Global state for the filesystem master
// ============================================================================
std::unordered_map<std::string, UserContext> active_users;
std::unordered_map<std::string, uint64_t> user_roots;
std::unordered_map<uint64_t, Inode> inode_table;
uint64_t next_block_id = 1;  // Start block IDs from 1
uint64_t next_inode_id = 0;  // Start inode IDs from 0
std::queue<uint64_t> free_inodes;
std::queue<uint64_t> free_block_ids;

// ============================================================================
// Mutexes for thread-safe access
// ============================================================================
std::mutex inode_allocation_mutex;
std::mutex block_allocation_mutex;

// ============================================================================
// Inode allocation: Reuses freed inodes or generates new ones
// Thread-safe: Protected by inode_allocation_mutex
// ============================================================================
uint64_t allocate_inode_id() {
    std::lock_guard<std::mutex> lock(inode_allocation_mutex);
    uint64_t inode_id;
    if (!free_inodes.empty()) {
        inode_id = free_inodes.front();
        free_inodes.pop();
    } else {
        inode_id = next_inode_id++;
    }
    return inode_id;
}

// ============================================================================
// Block allocation: Reuses freed blocks or generates new block IDs
// Thread-safe: Protected by block_allocation_mutex
// ============================================================================
uint64_t allocate_block_uuid() {
    std::lock_guard<std::mutex> lock(block_allocation_mutex);
    uint64_t block_id;
    if (!free_block_ids.empty()) {
        block_id = free_block_ids.front();
        free_block_ids.pop();
    } else {
        block_id = next_block_id++;
    }
    return block_id;
}

}  // namespace fs_master
