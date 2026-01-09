#include "fs_server/disk.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

namespace fs = std::filesystem;

namespace fs_server {

DiskStore::DiskStore(const std::string& blocks_dir)
    : blocks_dir_(blocks_dir) {
    // Create blocks directory if it doesn't exist
    if (!fs::exists(blocks_dir_)) {
        fs::create_directories(blocks_dir_);
        std::cout << "DiskStore: Created directory: " << blocks_dir_ << std::endl;
    }
}

DiskStore::~DiskStore() {
    // No explicit cleanup needed
}

std::string DiskStore::GetBlockPath(uint64_t block_uuid) const {
    std::stringstream ss;
    ss << blocks_dir_ << "/blk_" << block_uuid << ".img";
    return ss.str();
}

bool DiskStore::WriteBlock(uint64_t block_uuid, const std::string& data, bool sync) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        // Open file for writing (binary mode, truncate if exists)
        std::ofstream file(block_path, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            std::cerr << "DiskStore: Failed to open for writing: " << block_path
                      << std::endl;
            return false;
        }
        
        // Write entire block data to file
        file.write(data.c_str(), data.length());
        
        if (!file.good()) {
            std::cerr << "DiskStore: Write failed for: " << block_path << std::endl;
            file.close();
            return false;
        }
        
        // Flush C++ buffer to OS page cache
        file.flush();
        
        // If sync requested, force write to physical disk
        if (sync) {
            file.close();
            int fd = ::open(block_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                if (::fsync(fd) != 0) {
                    std::cerr << "DiskStore: fsync failed for: " << block_path
                              << " (errno: " << errno << ")" << std::endl;
                }
                ::close(fd);
            }
        } else {
            file.close();
        }
        
        // Track write statistics
        stats_.total_writes++;
        stats_.total_bytes_written += data.length();
        
        std::cout << "DiskStore: Wrote block " << block_uuid << " (" << data.length()
                  << " bytes, sync=" << (sync ? "true" : "false") << ")" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "DiskStore: Exception writing block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool DiskStore::ReadBlock(uint64_t block_uuid, std::string& out_data) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        // Check if file exists first
        if (!fs::exists(block_path)) {
            std::cerr << "DiskStore: Block not found: " << block_path << std::endl;
            return false;
        }
        
        // Open file for reading
        std::ifstream file(block_path, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "DiskStore: Failed to open for reading: " << block_path
                      << std::endl;
            return false;
        }
        
        // Get total file size
        file.seekg(0, std::ios::end);
        uint64_t total_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        // Read entire block
        std::vector<char> buffer(total_size);
        file.read(buffer.data(), total_size);
        
        uint64_t bytes_read = file.gcount();
        file.close();
        
        // Convert to string
        out_data.assign(buffer.begin(), buffer.begin() + bytes_read);
        
        // Track read statistics
        stats_.total_reads++;
        stats_.total_bytes_read += bytes_read;
        
        std::cout << "DiskStore: Read block " << block_uuid << " (" << bytes_read
                  << " bytes)" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "DiskStore: Exception reading block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool DiskStore::DeleteBlock(uint64_t block_uuid) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        if (!fs::exists(block_path)) {
            std::cerr << "DiskStore: Block file not found for deletion: "
                      << block_path << std::endl;
            return false;
        }
        
        fs::remove(block_path);
        std::cout << "DiskStore: Deleted block: " << block_uuid << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "DiskStore: Exception deleting block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool DiskStore::BlockExists(uint64_t block_uuid) {
    std::string block_path = GetBlockPath(block_uuid);
    return fs::exists(block_path);
}

uint64_t DiskStore::GetBlockSize(uint64_t block_uuid) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        if (!fs::exists(block_path)) {
            return 0;
        }
        return static_cast<uint64_t>(fs::file_size(block_path));
    } catch (const std::exception& e) {
        std::cerr << "DiskStore: Exception getting block size for block "
                  << block_uuid << ": " << e.what() << std::endl;
        return 0;
    }
}

DiskStore::AccessStats DiskStore::GetAccessStats() const {
    return {
        stats_.total_reads,
        stats_.total_writes,
        stats_.total_bytes_read,
        stats_.total_bytes_written
    };
}

void DiskStore::ResetAccessStats() {
    stats_.total_reads = 0;
    stats_.total_writes = 0;
    stats_.total_bytes_read = 0;
    stats_.total_bytes_written = 0;
}

}  // namespace fs_server

