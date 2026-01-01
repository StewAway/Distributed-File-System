#include "fs_master/fsmaster_service.hpp"
#include <iostream>
#include <algorithm>
#include <uuid/uuid.h>

namespace fs_master {

// ============================================================================
// DataNodeSelector Implementation
// ============================================================================

DataNodeSelector::DataNodeSelector(int replication_factor)
    : replication_factor_(replication_factor) {}

void DataNodeSelector::RegisterDataNode(
    const std::string& address,
    std::shared_ptr<FSServerService::Stub> stub) {
    
    DataNode node{address, stub, true};
    data_nodes_.push_back(node);
    std::cout << "Registered data node: " << address << std::endl;
}

std::vector<DataNodeSelector::DataNode*>
DataNodeSelector::SelectNodesForWrite(uint64_t block_uuid) {
    // SIMPLE STRATEGY: Round-robin replica placement
    // TODO: Future: Implement rack-aware placement (GFS style)
    
    std::vector<DataNodeSelector::DataNode*> selected;
    
    if (data_nodes_.empty()) {
        std::cerr << "No data nodes available for write!" << std::endl;
        return selected;
    }
    
    // Determine how many replicas to create
    int count = std::min(replication_factor_, (int)data_nodes_.size());
    
    // Select replicas in round-robin fashion
    for (int i = 0; i < count; ++i) {
        size_t idx = (round_robin_index_ + i) % data_nodes_.size();
        
        // TODO: Check if node is healthy before selecting
        if (data_nodes_[idx].is_healthy) {
            selected.push_back(&data_nodes_[idx]);
        }
    }
    
    // Move round-robin pointer forward for next write
    round_robin_index_ = (round_robin_index_ + count) % data_nodes_.size();
    
    std::cout << "Selected " << selected.size() << " nodes for block " 
              << block_uuid << std::endl;
    return selected;
}

DataNodeSelector::DataNode*
DataNodeSelector::SelectNodeForRead(uint64_t block_uuid) {
    // SIMPLE STRATEGY: Read from any healthy replica
    // TODO: Future: Implement load balancing based on latency/CPU
    
    if (data_nodes_.empty()) {
        return nullptr;
    }
    
    // For now, find first healthy node
    // In production: could do round-robin or weighted selection
    for (auto& node : data_nodes_) {
        if (node.is_healthy) {
            return &node;
        }
    }
    
    std::cerr << "No healthy nodes available for read!" << std::endl;
    return nullptr;
}

// ============================================================================
// FSMasterServiceImpl Implementation
// ============================================================================

FSMasterServiceImpl::FSMasterServiceImpl(
    std::shared_ptr<DataNodeSelector> selector)
    : data_node_selector_(selector) {
    std::cout << "FSMasterServiceImpl initialized" << std::endl;
}

grpc::Status FSMasterServiceImpl::Mount(
    grpc::ServerContext* context,
    const MountRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    
    // Check if user already mounted
    if (active_users.find(user_id) != active_users.end()) {
        response->set_success(true);
        response->set_error("User already mounted");
        std::cout << "User " << user_id << " already mounted" << std::endl;
        return grpc::Status::OK;
    }
    
    // Create new user context
    active_users[user_id] = UserContext();
    
    // Allocate root inode for this user (directory)
    uint64_t root_id = inode_table.size();
    while (free_inodes.empty()) {
        root_id = free_inodes.front();
        free_inodes.pop();
    }
    inode_table[root_id] = Inode(root_id, true);  // is_directory = true
    user_roots[user_id] = root_id;
    
    std::cout << "User " << user_id << " mounted with root inode " 
              << root_id << std::endl;
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Open(
    grpc::ServerContext* context,
    const OpenRequest* request,
    OpenResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    const std::string& mode = request->mode();
    
    // 1. Validate user is mounted
    if (active_users.find(user_id) == active_users.end()) {
        response->set_fd(-1);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto& user_ctx = active_users[user_id];
    uint64_t user_root = user_roots[user_id];
    
    // 2. Resolve path to find inode
    uint64_t inode_id = ResolvePath(path, user_root);
    
    // 3. Handle file not found
    if (inode_id == 0) {
        // Check if we're in write mode (can create file)
        if (mode.find('w') != std::string::npos || mode.find('a') != std::string::npos) {
            // Create new inode
            inode_id = inode_table.size();
            inode_table[inode_id] = Inode(inode_id, false);  // is_directory = false
            std::cout << "Created new file inode " << inode_id << " at " << path << std::endl;
        } else {
            response->set_fd(-1);
            response->set_error("File not found");
            return grpc::Status::OK;
        }
    }
    
    // 4. Allocate file descriptor
    int fd = ++user_ctx.fd_counter;
    
    FileSession session;
    session.inode_id = inode_id;
    session.offset = (mode.find('a') != std::string::npos) ? 
                     inode_table[inode_id].size : 0;
    session.mode = mode;
    
    user_ctx.open_files[fd] = session;
    
    std::cout << "Opened file at " << path << " with fd " << fd << std::endl;
    
    response->set_fd(fd);
    response->set_error("");
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Read(
    grpc::ServerContext* context,
    const ReadRequest* request,
    ReadResponse* response) {
    
    const std::string& user_id = request->user_id();
    int fd = request->fd();
    int count = request->count();
    
    // 1. Validate user and fd exist
    auto user_it = active_users.find(user_id);
    if (user_it == active_users.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "User not mounted");
    }
    
    auto fd_it = user_it->second.open_files.find(fd);
    if (fd_it == user_it->second.open_files.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "File descriptor not found");
    }
    
    auto& session = fd_it->second;
    auto& inode = inode_table[session.inode_id];
    
    // 2. Read data from blocks
    std::string data;
    size_t remaining = count;
    
    for (const auto& block_uuid_str : inode.blocks) {
        if (remaining == 0) break;
        
        uint64_t block_uuid = std::stoull(block_uuid_str);
        
        // SELECT NODE FOR READ using DataNodeSelector
        auto node = data_node_selector_->SelectNodeForRead(block_uuid);
        if (!node) {
            std::cerr << "No healthy nodes for reading block " << block_uuid << std::endl;
            continue;
        }
        
        // Call FSServerService::ReadBlock
        ReadBlockRequest req;
        req.set_block_uuid(block_uuid);
        
        ReadBlockResponse resp;
        grpc::ClientContext ctx;
        
        auto status = node->stub->ReadBlock(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "Failed to read block " << block_uuid << ": " 
                      << status.error_message() << std::endl;
            continue;
        }
        
        if (!resp.success()) {
            std::cerr << "Data node returned error for block " << block_uuid << std::endl;
            continue;
        }
        
        // Append block data
        const auto& block_data = resp.data();
        size_t to_read = std::min(remaining, (size_t)block_data.size());
        data.append(block_data.c_str(), to_read);
        remaining -= to_read;
    }
    
    // 3. Update file offset
    session.offset += data.size();
    
    // 4. Return response
    response->set_data(data);
    response->set_bytes_read(data.size());
    
    std::cout << "Read " << data.size() << " bytes from fd " << fd << std::endl;
    
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Write(
    grpc::ServerContext* context,
    const WriteRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    int fd = request->fd();
    const std::string& data = request->data();
    
    // 1. Validate user and fd
    auto user_it = active_users.find(user_id);
    if (user_it == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto fd_it = user_it->second.open_files.find(fd);
    if (fd_it == user_it->second.open_files.end()) {
        response->set_success(false);
        response->set_error("File descriptor not found");
        return grpc::Status::OK;
    }
    
    auto& session = fd_it->second;
    auto& inode = inode_table[session.inode_id];
    
    // 2. Generate unique block UUID
    // TODO: Use proper UUID generation library
    static uint64_t block_counter = 1000;
    uint64_t block_uuid = block_counter++;
    
    // 3. KEY STEP: SELECT NODES FOR REPLICATION
    auto nodes = data_node_selector_->SelectNodesForWrite(block_uuid);
    
    if (nodes.empty()) {
        response->set_success(false);
        response->set_error("No data nodes available");
        return grpc::Status::OK;
    }
    
    // 4. Write to all replicas
    // TODO: In production, make this parallel with threads/async
    bool all_success = true;
    for (auto node : nodes) {
        WriteBlockRequest req;
        req.set_block_uuid(block_uuid);
        req.set_data(data);
        
        StatusResponse resp;
        grpc::ClientContext ctx;
        
        auto status = node->stub->WriteBlock(&ctx, req, &resp);
        
        if (!status.ok()) {
            std::cerr << "Failed to write block " << block_uuid << " to node " 
                      << node->address << ": " << status.error_message() << std::endl;
            // TODO: Handle replica failure, try another node
            all_success = false;
        } else if (!resp.success()) {
            std::cerr << "Data node returned error for block write" << std::endl;
            all_success = false;
        }
    }
    
    if (!all_success) {
        // In production: may want to retry or partial success
        response->set_success(false);
        response->set_error("Replication partially failed");
        return grpc::Status::OK;
    }
    
    // 5. Update inode metadata
    inode.blocks.push_back(std::to_string(block_uuid));
    inode.size += data.size();
    session.offset += data.size();
    
    std::cout << "Wrote " << data.size() << " bytes to fd " << fd 
              << " with replication factor " << nodes.size() << std::endl;
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Close(
    grpc::ServerContext* context,
    const CloseRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    // TODO: Find fd from path and remove from active files
    // For now, just return success
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Mkdir(
    grpc::ServerContext* context,
    const MkdirRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    if (active_users.find(user_id) == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    // Create new directory inode
    uint64_t dir_id = inode_table.size();
    inode_table[dir_id] = Inode(dir_id, true);  // is_directory = true
    
    // TODO: Link in parent directory properly
    // For now, just return success
    
    std::cout << "Created directory at " << path << " with inode " << dir_id << std::endl;
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Rmdir(
    grpc::ServerContext* context,
    const RmdirRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    // TODO: Check directory is empty, then delete inode
    
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Ls(
    grpc::ServerContext* context,
    const LsRequest* request,
    LsResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    if (active_users.find(user_id) == active_users.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "User not mounted");
    }
    
    // TODO: Find inode for path, return list of children
    
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::DeleteFile(
    grpc::ServerContext* context,
    const DeleteFileRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    // TODO: Delete file and notify FSServers to free blocks
    
    response->set_success(true);
    return grpc::Status::OK;
}

// ============================================================================
// Helper Methods
// ============================================================================

uint64_t FSMasterServiceImpl::ResolvePath(const std::string& path, uint64_t user_root) {
    // Handle root path
    if (path == "/" || path.empty()) {
        return user_root;
    }
    
    // TODO: Implement proper path navigation through inode tree
    // For now, return user_root
    
    return user_root;
}

grpc::Status FSMasterServiceImpl::WriteBlockToFSServers(
    uint64_t block_uuid,
    const std::string& data) {
    
    // This is implemented in Write() method
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::ReadBlockFromFSServer(
    uint64_t block_uuid,
    std::string& data) {
    
    auto node = data_node_selector_->SelectNodeForRead(block_uuid);
    if (!node) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, 
                          "No healthy data nodes available");
    }
    
    ReadBlockRequest req;
    req.set_block_uuid(block_uuid);
    
    ReadBlockResponse resp;
    grpc::ClientContext ctx;
    
    auto status = node->stub->ReadBlock(&ctx, req, &resp);
    
    if (!status.ok() || !resp.success()) {
        return grpc::Status(grpc::StatusCode::INTERNAL, 
                          "Failed to read block from data node");
    }
    
    data = resp.data();
    return grpc::Status::OK;
}

}  // namespace fs_master
