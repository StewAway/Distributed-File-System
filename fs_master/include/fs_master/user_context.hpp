#pragma once
#include <unordered_map>
#include <string>
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

extern std::unordered_map<std::string, UserContext> active_users;
extern std::unordered_map<std::string, uint64_t> user_roots;
extern std::unordered_map<uint64_t, Inode> inode_table;

}