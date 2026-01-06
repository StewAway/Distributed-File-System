#include "fs_server/block_store.hpp"
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

BlockStore::BlockStore(const std::string& blocks_dir)
    : blocks_dir_(blocks_dir) {
    // Create blocks directory if it doesn't exist
    if (!fs::exists(blocks_dir_)) {
        fs::create_directories(blocks_dir_);
        std::cout << "BlockStore: Created directory: " << blocks_dir_ << std::endl;
    }
}

BlockStore::~BlockStore() {
    // No explicit cleanup needed
}

std::string BlockStore::GetBlockPath(uint64_t block_uuid) const {
    std::stringstream ss;
    ss << blocks_dir_ << "/blk_" << block_uuid << ".img";
    return ss.str();
}

bool BlockStore::WriteBlockToDisk(uint64_t block_uuid, const std::string& data,
                                   bool sync) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        // Open file for writing (binary mode, truncate if exists)
        std::ofstream file(block_path, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            std::cerr << "BlockStore: Failed to open for writing: " << block_path
                      << std::endl;
            return false;
        }
        
        // Write data to file
        // This data goes into C++ library's internal buffer
        file.write(data.c_str(), data.length());
        
        if (!file.good()) {
            std::cerr << "BlockStore: Write failed for: " << block_path << std::endl;
            file.close();
            return false;
        }
        
        // Flush C++ buffer to OS page cache
        file.flush();
        
        // If sync requested, force write to physical disk
        if (sync) {
            file.close();
            // Use system-level fsync on the written file
            int fd = ::open(block_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                if (::fsync(fd) != 0) {
                    std::cerr << "BlockStore: fsync failed for: " << block_path
                              << " (errno: " << errno << ")" << std::endl;
                }
                ::close(fd);
            }
        } else {
            file.close();
        }
        
        // Tier 2: Track write statistics
        stats_.total_writes++;
        stats_.total_bytes_written += data.length();
        
        std::cout << "BlockStore: Wrote block " << block_uuid << " (" << data.length()
                  << " bytes, sync=" << (sync ? "true" : "false") << ")" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception writing block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::ReadBlockFromDisk(uint64_t block_uuid, uint32_t offset,
                                    uint32_t length, std::string& out_data) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        // Check if file exists first
        if (!fs::exists(block_path)) {
            std::cerr << "BlockStore: Block file not found: " << block_path
                      << std::endl;
            return false;
        }
        
        // Open file for reading
        std::ifstream file(block_path, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "BlockStore: Failed to open for reading: " << block_path
                      << std::endl;
            return false;
        }
        
        // Get total file size
        file.seekg(0, std::ios::end);
        uint32_t total_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        // Validate offset
        if (offset > total_size) {
            std::cerr << "BlockStore: Offset " << offset << " exceeds file size "
                      << total_size << std::endl;
            file.close();
            out_data.clear();
            return true;  // Not an error, just empty read
        }
        
        // Calculate actual read length
        uint32_t actual_length = length;
        if (length == 0) {
            // Read from offset to end
            actual_length = total_size - offset;
        } else {
            // Clamp to available data
            uint32_t max_available = total_size - offset;
            actual_length = std::min(length, max_available);
        }
        
        // Seek to offset and read
        file.seekg(offset);
        
        // Allocate buffer and read
        std::vector<char> buffer(actual_length);
        file.read(buffer.data(), actual_length);
        
        uint32_t bytes_read = file.gcount();
        
        if (bytes_read < 0) {
            std::cerr << "BlockStore: Read failed for: " << block_path << std::endl;
            file.close();
            return false;
        }
        
        // Convert to string
        out_data.assign(buffer.begin(), buffer.begin() + bytes_read);
        
        file.close();
        
        // Tier 2: Track read statistics
        stats_.total_reads++;
        stats_.total_bytes_read += bytes_read;
        
        std::cout << "BlockStore: Read block " << block_uuid << " (" << bytes_read
                  << " bytes, offset=" << offset << ", length=" << length << ")"
                  << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception reading block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::DeleteBlockFromDisk(uint64_t block_uuid) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        if (!fs::exists(block_path)) {
            std::cerr << "BlockStore: Block file not found for deletion: "
                      << block_path << std::endl;
            return false;
        }
        
        fs::remove(block_path);
        std::cout << "BlockStore: Deleted block: " << block_uuid << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception deleting block " << block_uuid << ": "
                  << e.what() << std::endl;
        return false;
    }
}

bool BlockStore::BlockFileExists(uint64_t block_uuid) {
    std::string block_path = GetBlockPath(block_uuid);
    return fs::exists(block_path);
}

uint32_t BlockStore::GetBlockFileSize(uint64_t block_uuid) {
    try {
        std::string block_path = GetBlockPath(block_uuid);
        if (!fs::exists(block_path)) {
            return 0;
        }
        return static_cast<uint32_t>(fs::file_size(block_path));
    } catch (const std::exception& e) {
        std::cerr << "BlockStore: Exception getting file size for block "
                  << block_uuid << ": " << e.what() << std::endl;
        return 0;
    }
}

BlockStore::AccessStats BlockStore::GetAccessStats() const {
    return {
        stats_.total_reads,
        stats_.total_writes,
        stats_.total_bytes_read,
        stats_.total_bytes_written
    };
}

void BlockStore::ResetAccessStats() {
    stats_.total_reads = 0;
    stats_.total_writes = 0;
    stats_.total_bytes_read = 0;
    stats_.total_bytes_written = 0;
}

}  // namespace fs_server
