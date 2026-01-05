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

// ============================================================================
// Thread-safe accessor functions for inode_table
// ============================================================================
std::optional<Inode> GetInode(uint64_t inode_id);
bool InodeExists(uint64_t inode_id);
void PutInode(uint64_t inode_id, const Inode& inode);
bool DeleteInode(uint64_t inode_id);
size_t GetInodeTableSize();

}