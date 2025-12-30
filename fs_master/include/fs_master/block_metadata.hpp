#pragma once
#include <string>
#include <vector>
#include <unordered_map>

namespace fs_master {

struct BlockMetadata {
    std::string uuid;
    std::vector<std::string> replica_hosts;
};

extern std::unordered_map<std::string, BlockMetadata> block_map;

}