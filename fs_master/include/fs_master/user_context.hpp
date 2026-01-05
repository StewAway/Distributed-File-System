#pragma once
#include <unordered_map>
#include <queue>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include "fs_master/inode.hpp"

namespace fs_master {

struct FileSession {
    uint64_t inode_id;
    uint64_t offset;
    std::string mode;
};

struct UserContext {
    int fd_counter = 0;
    std::unordered_map<int, FileSession> open_files;
};

uint64_t allocate_inode_id();
uint64_t allocate_block_uuid();

extern std::unordered_map<std::string, UserContext> active_users;
extern std::unordered_map<std::string, uint64_t> user_roots;
extern std::unordered_map<uint64_t, Inode> inode_table;
extern std::queue<uint64_t> free_inodes;
extern uint64_t next_block_id;
extern uint64_t next_inode_id;
extern std::queue<uint64_t> free_block_ids;

// ============================================================================
// Mutexes for thread-safe access to global state
// ============================================================================
extern std::mutex inode_allocation_mutex;
extern std::mutex block_allocation_mutex;
extern std::shared_mutex inode_table_mutex;
extern std::shared_mutex active_users_mutex;
extern std::shared_mutex user_roots_mutex;
// ============================================================================
extern std::mutex inode_allocation_mutex;
extern std::mutex block_allocation_mutex;
extern std::shared_mutex inode_table_mutex;
extern std::shared_mutex active_users_mutex;
extern std::shared_mutex user_roots_mutex;

// ============================================================================
// Thread-safe accessor functions for inode_table
// ============================================================================

/**
 * Get an inode by ID (read operation - multiple threads can read simultaneously)
 * Returns a copy of the inode if found, or std::nullopt if not found
 */
std::optional<Inode> GetInode(uint64_t inode_id);

/**
 * Check if an inode exists
 */
bool InodeExists(uint64_t inode_id);

/**
 * Create or update an inode (write operation - exclusive access)
 */
void PutInode(uint64_t inode_id, const Inode& inode);

/**
 * Delete an inode
 */
bool DeleteInode(uint64_t inode_id);

/**
 * Get the current number of inodes
 */
size_t GetInodeTableSize();

// ============================================================================
// Thread-safe accessor functions for active_users
// ============================================================================

/**
 * Check if a user is mounted
 */
bool UserExists(const std::string& user_id);

/**
 * Get user context (read operation)
 */
std::optional<UserContext> GetUserContext(const std::string& user_id);

/**
 * Create or update user context (write operation)
 */
void PutUserContext(const std::string& user_id, const UserContext& context);

/**
 * Remove a user from active_users
 */
bool RemoveUser(const std::string& user_id);

// ============================================================================
// Thread-safe accessor functions for user_roots
// ============================================================================

/**
 * Get user's root inode ID
 */
std::optional<uint64_t> GetUserRoot(const std::string& user_id);

/**
 * Set user's root inode ID
 */
void SetUserRoot(const std::string& user_id, uint64_t root_id);

/**
 * Check if user root exists
 */
bool UserRootExists(const std::string& user_id);

// ============================================================================
// Combined helper for validating user and getting context + root
// ============================================================================

struct UserContextAndRoot {
    UserContext context;
    uint64_t root_id;
};

/**
 * Get user context and root ID together (atomic read operation)
 * Returns std::nullopt if user doesn't exist or isn't properly initialized
 */
std::optional<UserContextAndRoot> GetUserContextAndRoot(const std::string& user_id);

}