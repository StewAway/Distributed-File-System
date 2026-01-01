#include "fs_master/inode.hpp"

namespace fs_master {

Inode::Inode(uint64_t id, bool is_dir)
    : id(id), is_directory(is_dir), size(0) {}

}  // namespace fs_master
