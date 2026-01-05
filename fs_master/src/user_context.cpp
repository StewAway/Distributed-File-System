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
std::shared_mutex inode_table_mutex;
std::shared_mutex active_users_mutex;
std::shared_mutex user_roots_mutex;

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

// ============================================================================
// Thread-safe accessor functions for inode_table
// ============================================================================

std::optional<Inode> GetInode(uint64_t inode_id) {
    std::shared_lock<std::shared_mutex> lock(inode_table_mutex);
    auto it = inode_table.find(inode_id);
    if (it != inode_table.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool InodeExists(uint64_t inode_id) {
    std::shared_lock<std::shared_mutex> lock(inode_table_mutex);
    return inode_table.find(inode_id) != inode_table.end();
}

void PutInode(uint64_t inode_id, const Inode& inode) {
    std::unique_lock<std::shared_mutex> lock(inode_table_mutex);
    inode_table[inode_id] = inode;
}

bool DeleteInode(uint64_t inode_id) {
    std::unique_lock<std::shared_mutex> lock(inode_table_mutex);
    auto it = inode_table.find(inode_id);
    if (it != inode_table.end()) {
        inode_table.erase(it);
        return true;
    }
    return false;
}

size_t GetInodeTableSize() {
    std::shared_lock<std::shared_mutex> lock(inode_table_mutex);
    return inode_table.size();
}

// ============================================================================
// Thread-safe accessor functions for active_users
// ============================================================================

bool UserExists(const std::string& user_id) {
    std::shared_lock<std::shared_mutex> lock(active_users_mutex);
    return active_users.find(user_id) != active_users.end();
}

std::optional<UserContext> GetUserContext(const std::string& user_id) {
    std::shared_lock<std::shared_mutex> lock(active_users_mutex);
    auto it = active_users.find(user_id);
    if (it != active_users.end()) {
        return it->second;
    }
    return std::nullopt;
}

void PutUserContext(const std::string& user_id, const UserContext& context) {
    std::unique_lock<std::shared_mutex> lock(active_users_mutex);
    active_users[user_id] = context;
}

bool RemoveUser(const std::string& user_id) {
    std::unique_lock<std::shared_mutex> lock(active_users_mutex);
    auto it = active_users.find(user_id);
    if (it != active_users.end()) {
        active_users.erase(it);
        return true;
    }
    return false;
}

// ============================================================================
// Thread-safe accessor functions for user_roots
// ============================================================================

std::optional<uint64_t> GetUserRoot(const std::string& user_id) {
    std::shared_lock<std::shared_mutex> lock(user_roots_mutex);
    auto it = user_roots.find(user_id);
    if (it != user_roots.end()) {
        return it->second;
    }
    return std::nullopt;
}

void SetUserRoot(const std::string& user_id, uint64_t root_id) {
    std::unique_lock<std::shared_mutex> lock(user_roots_mutex);
    user_roots[user_id] = root_id;
}

bool UserRootExists(const std::string& user_id) {
    std::shared_lock<std::shared_mutex> lock(user_roots_mutex);
    return user_roots.find(user_id) != user_roots.end();
}

std::optional<UserContextAndRoot> GetUserContextAndRoot(const std::string& user_id) {
    // Lock both mutexes to ensure consistency
    std::shared_lock<std::shared_mutex> lock1(active_users_mutex);
    std::shared_lock<std::shared_mutex> lock2(user_roots_mutex);
    
    auto user_it = active_users.find(user_id);
    auto root_it = user_roots.find(user_id);
    
    if (user_it != active_users.end() && root_it != user_roots.end()) {
        return UserContextAndRoot{user_it->second, root_it->second};
    }
    
    return std::nullopt;
}

}  // namespace fs_master
