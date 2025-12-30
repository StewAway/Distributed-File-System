#pragma once
#include <string>
#include <unordered_map>
#include <vector>

namespace fs_master {

struct Inode {
    uint64_t id;
    bool is_directory;
    uint64_t size;
    std::vector<std::string> blocks;
    std::unordered_map<std::string, uint64_t> children;

    Inode(uint64_t id = 0, bool is_dir = true);
};

}