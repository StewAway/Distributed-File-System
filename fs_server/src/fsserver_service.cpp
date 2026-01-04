#include "fs_server/fsserver_service.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <openssl/sha.h>
#include <ctime>

namespace fs = std::filesystem;

namespace fs_server {

// ============================================================================
// BlockManager Implementation
// ============================================================================

BlockManager::BlockManager(const std::string& blocks_dir)
    : blocks_dir_(blocks_dir) {
    // Create blocks directory if it doesn't exist
    if (!fs::exists(blocks_dir_)) {
        fs::create_directories(blocks_dir_);
        std::cout << "Created blocks directory: " << blocks_dir_ << std::endl;
    }
    
    // Load existing blocks from disk
    LoadExistingBlocks();
}

BlockManager::~BlockManager() {
    // Cleanup on destruction
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    std::cout << "BlockManager destroyed. Stored " << blocks_map_.size() 
              << " blocks." << std::endl;
}

std::string BlockManager::GetBlockPath(uint64_t block_uuid) const {
    std::stringstream ss;
    ss << blocks_dir_ << "/blk_" << block_uuid << ".img";
    return ss.str();
}

std::string BlockManager::CalculateChecksum(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), hash);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

std::string BlockManager::GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&time), "%Y-%m-%dT%H:%M:%S")
       << "." << std::setfill('0') << std::setw(3) << ms.count() << "Z";
    return ss.str();
}

void BlockManager::LoadExistingBlocks() {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    
    try {
        for (const auto& entry : fs::directory_iterator(blocks_dir_)) {
            if (entry.is_regular_file() && entry.path().extension() == ".img") {
                // Extract UUID from filename (format: blk_<uuid>.img)
                std::string filename = entry.path().filename().string();
                size_t start = filename.find("blk_") + 4;
                size_t end = filename.find(".img");
                std::string uuid_str = filename.substr(start, end - start);
                
                uint64_t block_uuid = std::stoull(uuid_str);
                uint32_t size = static_cast<uint32_t>(entry.file_size());
                std::string created_at = GetCurrentTimestamp();
                
                // Read file and calculate checksum
                std::ifstream file(entry.path(), std::ios::binary);
                std::string data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
                file.close();
                
                std::string checksum = CalculateChecksum(data);
                
                blocks_map_[block_uuid] = BlockMetadata(block_uuid, size, created_at, checksum);
                std::cout << "Loaded block: " << block_uuid << " (size: " << size << " bytes)" << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error loading existing blocks: " << e.what() << std::endl;
    }
}

bool BlockManager::WriteBlock(uint64_t block_uuid, const std::string& data,
                              uint32_t offset, bool sync) {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    std::cout << "Writing block " << block_uuid << " (size: " 
              << data.length() << " bytes, offset: " << offset << ")" << std::endl;
    // Validate block size
    if (data.length() > BLOCK_SIZE) {
        std::cerr << "Data exceeds max block size: " << data.length() 
                  << " > " << BLOCK_SIZE << std::endl;
        return false;
    }
    
    try {
        std::string block_path = GetBlockPath(block_uuid);
        
        // Write block data to disk
        std::ofstream file(block_path, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open file for writing: " << block_path << std::endl;
            return false;
        }
        
        file.write(data.c_str(), data.length());
        
        // Optionally force fsync for durability
        if (sync) {
            file.flush();
            // In production, would call fsync(fileno(file))
        }
        file.close();
        
        // Calculate checksum
        std::string checksum = CalculateChecksum(data);
        std::string created_at = GetCurrentTimestamp();
        
        // Update metadata
        blocks_map_[block_uuid] = BlockMetadata(block_uuid, data.length(), 
                                                created_at, checksum);
        
        std::cout << "Write block " << block_uuid << ": " << data.length() 
                  << " bytes [" << checksum.substr(0, 8) << "...]" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error writing block: " << e.what() << std::endl;
        return false;
    }
}

bool BlockManager::ReadBlock(uint64_t block_uuid, uint32_t offset, uint32_t length,
                             std::string& out_data) {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    
    try {
        // Check if block exists
        auto it = blocks_map_.find(block_uuid);
        if (it == blocks_map_.end()) {
            std::cerr << "Block not found: " << block_uuid << std::endl;
            return false;
        }
        
        // Update access count (for future page cache optimization)
        it->second.access_count++;
        
        std::string block_path = GetBlockPath(block_uuid);
        std::ifstream file(block_path, std::ios::binary);
        
        if (!file.is_open()) {
            std::cerr << "Failed to open file for reading: " << block_path << std::endl;
            return false;
        }
        
        // Read entire block
        std::string data((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
        file.close();
        
        // Handle partial reads with offset (for future page cache)
        uint32_t read_length = length;
        if (length == 0) {
            // Read from offset to end of block
            read_length = data.length() - std::min(offset, (uint32_t)data.length());
        }
        
        if (offset >= data.length()) {
            out_data.clear();
            return true;
        }
        
        // Extract requested range
        out_data = data.substr(offset, read_length);
        
        std::cout << "Read block " << block_uuid << ": " << out_data.length() 
                  << " bytes (offset: " << offset << ", len: " << length << ")" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error reading block: " << e.what() << std::endl;
        return false;
    }
}

bool BlockManager::DeleteBlock(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    
    try {
        // Remove from metadata map
        auto it = blocks_map_.find(block_uuid);
        if (it == blocks_map_.end()) {
            std::cerr << "Block not found for deletion: " << block_uuid << std::endl;
            return false;
        }
        
        // Delete file from disk
        std::string block_path = GetBlockPath(block_uuid);
        if (fs::exists(block_path)) {
            fs::remove(block_path);
            blocks_map_.erase(it);
            std::cout << "Deleted block: " << block_uuid << std::endl;
            return true;
        } else {
            std::cerr << "Block file not found: " << block_path << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error deleting block: " << e.what() << std::endl;
        return false;
    }
}

bool BlockManager::BlockExists(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    return blocks_map_.find(block_uuid) != blocks_map_.end();
}

bool BlockManager::GetBlockMetadata(uint64_t block_uuid, BlockMetadata& out_metadata) {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    
    auto it = blocks_map_.find(block_uuid);
    if (it == blocks_map_.end()) {
        return false;
    }
    
    out_metadata = it->second;
    return true;
}

std::vector<uint64_t> BlockManager::GetAllBlocks() {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    
    std::vector<uint64_t> blocks;
    for (const auto& pair : blocks_map_) {
        blocks.push_back(pair.first);
    }
    return blocks;
}

uint64_t BlockManager::GetTotalStorageUsed() {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    
    uint64_t total = 0;
    for (const auto& pair : blocks_map_) {
        total += pair.second.size;
    }
    return total;
}

// ============================================================================
// FSServerServiceImpl Implementation
// ============================================================================

FSServerServiceImpl::FSServerServiceImpl(const std::string& datanode_id,
                                       const std::string& blocks_dir)
    : datanode_id_(datanode_id) {
    block_manager_ = std::make_unique<BlockManager>(blocks_dir);
    std::cout << "Initialized FSServerServiceImpl with ID: " << datanode_id_ << std::endl;
}

grpc::Status FSServerServiceImpl::ReadBlock(grpc::ServerContext* context,
                                          const ReadBlockRequest* request,
                                          ReadBlockResponse* response) {
    std::string data;
    bool success = block_manager_->ReadBlock(request->block_uuid(),
                                             request->offset(),
                                             request->length(),
                                             data);
    
    response->set_success(success);
    response->set_data(data);
    response->set_bytes_read(data.length());
    
    if (!success) {
        response->set_error("Failed to read block " + std::to_string(request->block_uuid()));
    }
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        request_count_++;
    }
    
    return grpc::Status::OK;
}

grpc::Status FSServerServiceImpl::WriteBlock(grpc::ServerContext* context,
                                           const WriteBlockRequest* request,
                                           StatusResponse* response) {
    bool success = block_manager_->WriteBlock(request->block_uuid(),
                                              request->data(),
                                              request->offset(),
                                              request->sync());
    
    response->set_success(success);
    if (!success) {
        response->set_error("Failed to write block " + std::to_string(request->block_uuid()));
    }
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        request_count_++;
    }
    
    return grpc::Status::OK;
}

grpc::Status FSServerServiceImpl::DeleteBlock(grpc::ServerContext* context,
                                            const DeleteBlockRequest* request,
                                            StatusResponse* response) {
    bool success = block_manager_->DeleteBlock(request->block_uuid());
    
    response->set_success(success);
    if (!success) {
        response->set_error("Failed to delete block " + std::to_string(request->block_uuid()));
    }
    
    return grpc::Status::OK;
}

grpc::Status FSServerServiceImpl::GetBlockInfo(grpc::ServerContext* context,
                                             const GetBlockInfoRequest* request,
                                             GetBlockInfoResponse* response) {
    BlockMetadata metadata;
    bool exists = block_manager_->GetBlockMetadata(request->block_uuid(), metadata);
    
    response->set_exists(exists);
    if (exists) {
        response->set_size(metadata.size);
        response->set_created_at(metadata.created_at);
        response->set_checksum(metadata.checksum);
    }
    
    return grpc::Status::OK;
}

grpc::Status FSServerServiceImpl::HeartBeat(grpc::ServerContext* context,
                                          const HeartBeatRequest* request,
                                          HeartBeatResponse* response) {
    auto blocks = block_manager_->GetAllBlocks();
    
    std::cout << "HeartBeat from datanode " << request->datanode_id() 
              << ": " << blocks.size() << " blocks stored" << std::endl;
    
    response->set_success(true);
    
    return grpc::Status::OK;
}

std::string FSServerServiceImpl::GetStatistics() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    uint64_t total_storage = block_manager_->GetTotalStorageUsed();
    auto blocks = block_manager_->GetAllBlocks();
    
    std::stringstream ss;
    ss << "=== Datanode Statistics ===" << std::endl
       << "Datanode ID: " << datanode_id_ << std::endl
       << "Total Blocks: " << blocks.size() << std::endl
       << "Total Storage Used: " << total_storage << " bytes "
       << "(" << (total_storage / (1024 * 1024)) << " MB)" << std::endl
       << "Total Requests: " << request_count_ << std::endl;
    
    return ss.str();
}

}  // namespace fs_server
