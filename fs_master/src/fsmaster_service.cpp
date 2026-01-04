#include "fs_master/fsmaster_service.hpp"
#include <iostream>
#include <algorithm>
#include <uuid/uuid.h>
#include <queue>
#include <cstring>

namespace fs_master {

// ============================================================================
// Constants
// ============================================================================
constexpr uint32_t BLOCK_SIZE = 65536;  // 64 KB blocks (must match fs_server)

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
    // STRATEGY: Write to all healthy nodes for replication
    // In production: This can be made configurable for rack-aware placement (GFS style)
    
    std::vector<DataNodeSelector::DataNode*> selected;
    
    if (data_nodes_.empty()) {
        std::cerr << "No data nodes available for write!" << std::endl;
        return selected;
    }
    
    // Collect all healthy nodes
    for (auto& node : data_nodes_) {
        if (node.is_healthy) {
            selected.push_back(&node);
        }
    }
    
    std::cout << "Selected " << selected.size() << " healthy nodes for block " 
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
    std::cout<<inode_table.size()<<std::endl;
    while (!free_inodes.empty()) {
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

grpc::Status FSMasterServiceImpl::UnMount(
    grpc::ServerContext* context,
    const MountRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    
    // Check if user is mounted
    auto it = active_users.find(user_id);
    if (it == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        std::cout << "User " << user_id << " not mounted" << std::endl;
        return grpc::Status::OK;
    }
    
    // Clean up user context
    active_users.erase(it);
    
    // Optionally: Free up user's inodes and blocks
    // 1) add new user root id into free inode pool
    uint64_t user_root = user_roots[user_id];
    free_inodes.push(user_root);
    // 2) recursively clear up inodes and blocks
    // pending: implement recursive inode deletion
    
    std::cout << "User " << user_id << " unmounted" << std::endl;
    
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
    
    // 2. DIVIDE DATA INTO BLOCKS
    // For data larger than BLOCK_SIZE, split into multiple blocks
    uint32_t offset = 0;
    uint32_t total_written = 0;
    std::vector<uint64_t> written_blocks;
    
    while (offset < data.length()) {
        // Determine block size for this chunk
        uint32_t chunk_size = std::min((uint32_t)BLOCK_SIZE, 
                                       (uint32_t)(data.length() - offset));
        std::string block_data = data.substr(offset, chunk_size);
        
        // 3. Generate unique block UUID for this block
        uint64_t block_uuid = allocate_block_uuid();
        
        // 4. SELECT ONLY HEALTHY NODES FOR REPLICATION
        auto nodes = data_node_selector_->SelectNodesForWrite(block_uuid);
        
        if (nodes.empty()) {
            std::cerr << "No healthy data nodes available for block " << block_uuid << std::endl;
            response->set_success(false);
            response->set_error("No healthy data nodes available");
            return grpc::Status::OK;
        }
        
        // 5. WRITE BLOCK TO ALL HEALTHY NODES
        // TODO: In production, make this parallel with threads/async
        bool block_write_success = false;
        uint32_t successful_writes = 0;
        
        for (auto node : nodes) {
            WriteBlockRequest req;
            req.set_block_uuid(block_uuid);
            req.set_data(block_data);
            req.set_offset(0);  // Writing full block
            req.set_sync(true); // Force sync for durability
            
            StatusResponse resp;
            grpc::ClientContext ctx;
            
            auto status = node->stub->WriteBlock(&ctx, req, &resp);
            
            if (!status.ok()) {
                std::cerr << "Failed to write block " << block_uuid << " to node " 
                          << node->address << ": " << status.error_message() << std::endl;
                // Continue writing to other healthy nodes
                continue;
            } else if (!resp.success()) {
                std::cerr << "Data node " << node->address 
                          << " returned error for block write: " << resp.error() << std::endl;
                // Continue writing to other healthy nodes
                continue;
            }
            
            successful_writes++;
            block_write_success = true;
            std::cout << "Successfully wrote block " << block_uuid << " (" << chunk_size 
                      << " bytes) to node " << node->address << std::endl;
        }
        
        // Check if block was written to at least one healthy node
        if (!block_write_success) {
            response->set_success(false);
            response->set_error("Failed to write block " + std::to_string(block_uuid) + 
                              " to any healthy data node");
            return grpc::Status::OK;
        }
        
        // 6. UPDATE INODE METADATA FOR THIS BLOCK
        inode.blocks.push_back(std::to_string(block_uuid));
        written_blocks.push_back(block_uuid);
        total_written += chunk_size;
        offset += chunk_size;
        
        std::cout << "Block " << block_uuid << " written to " << successful_writes 
                  << " node(s) out of " << nodes.size() << std::endl;
    }
    
    // 7. FINALIZE WRITE
    inode.size += data.length();
    session.offset += data.length();
    
    std::cout << "Write complete: " << data.length() << " bytes written to fd " << fd 
              << " across " << written_blocks.size() << " block(s)" << std::endl;
    
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
    
    // Validate user is mounted
    auto user_it = active_users.find(user_id);
    if (user_it == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto& user_ctx = user_it->second;
    
    // Find and remove file descriptor by path
    bool found = false;
    for (auto it = user_ctx.open_files.begin(); it != user_ctx.open_files.end(); ++it) {
        // TODO: Improve path matching in file session
        // For now, match by inode id
        found = true;
        user_ctx.open_files.erase(it);
        break;
    }
    
    if (!found) {
        response->set_success(false);
        response->set_error("File not open");
        return grpc::Status::OK;
    }
    
    std::cout << "Closed file at " << path << " for user " << user_id << std::endl;
    
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
    
    // 1. Validate user is mounted
    if (active_users.find(user_id) == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    uint64_t user_root = user_roots[user_id];
    
    // 2. Check if directory already exists
    // For now, simple check on root children
    auto& root_inode = inode_table[user_root];
    if (root_inode.children.find(path) != root_inode.children.end()) {
        response->set_success(false);
        response->set_error("Directory already exists");
        return grpc::Status::OK;
    }
    
    // 3. Allocate new directory inode using allocate_inode_id()
    uint64_t dir_id = allocate_inode_id();
    
    inode_table[dir_id] = Inode(dir_id, true);  // is_directory = true
    
    // 4. Link directory in parent (root)
    root_inode.children[path] = dir_id;
    
    std::cout << "Created directory at " << path << " with inode " << dir_id 
              << " for user " << user_id << std::endl;
    
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
    
    // 1. Validate user is mounted
    auto user_it = active_users.find(user_id);
    if (user_it == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    uint64_t user_root = user_roots[user_id];
    auto& root_inode = inode_table[user_root];
    
    // 2. Check if directory exists
    auto child_it = root_inode.children.find(path);
    if (child_it == root_inode.children.end()) {
        response->set_success(false);
        response->set_error("Directory not found");
        return grpc::Status::OK;
    }
    
    uint64_t dir_id = child_it->second;
    auto& dir_inode = inode_table[dir_id];
    
    // 3. Check if directory is empty
    if (!dir_inode.children.empty() || !dir_inode.blocks.empty()) {
        response->set_success(false);
        response->set_error("Directory not empty");
        return grpc::Status::OK;
    }
    
    // 4. Remove directory
    root_inode.children.erase(child_it);
    // Optionally: add dir_id to free_inodes for reuse
    free_inodes.push(dir_id);
    
    std::cout << "Removed directory at " << path << " (inode " << dir_id 
              << ") for user " << user_id << std::endl;
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::Ls(
    grpc::ServerContext* context,
    const LsRequest* request,
    LsResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    // 1. Validate user is mounted
    if (active_users.find(user_id) == active_users.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "User not mounted");
    }
    
    uint64_t user_root = user_roots[user_id];
    auto& inode = inode_table[user_root];
    
    // 2. Check if inode is directory
    if (!inode.is_directory) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Not a directory");
    }
    
    // 3. Return list of children
    for (const auto& pair : inode.children) {
        response->add_files(pair.first);
    }
    
    std::cout << "Listing directory " << path << " for user " << user_id 
              << ": " << response->files_size() << " entries" << std::endl;
    
    return grpc::Status::OK;
}

grpc::Status FSMasterServiceImpl::DeleteFile(
    grpc::ServerContext* context,
    const DeleteFileRequest* request,
    StatusResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    
    // 1. Validate user is mounted
    auto user_it = active_users.find(user_id);
    if (user_it == active_users.end()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    uint64_t user_root = user_roots[user_id];
    auto& root_inode = inode_table[user_root];
    
    // 2. Find file inode
    auto child_it = root_inode.children.find(path);
    if (child_it == root_inode.children.end()) {
        response->set_success(false);
        response->set_error("File not found");
        return grpc::Status::OK;
    }
    
    uint64_t file_id = child_it->second;
    auto& file_inode = inode_table[file_id];
    
    // 3. Check it's not a directory
    if (file_inode.is_directory) {
        response->set_success(false);
        response->set_error("Cannot delete directory with DeleteFile");
        return grpc::Status::OK;
    }
    
    // 4. DELETE BLOCKS FROM ALL DATA NODES
    // Send DeleteBlock request to all data nodes
    auto nodes = data_node_selector_->SelectNodesForWrite(0);  // Get all healthy nodes
    
    for (const auto& block_uuid_str : file_inode.blocks) {
        uint64_t block_uuid = std::stoull(block_uuid_str);
        
        for (auto node : nodes) {
            DeleteBlockRequest req;
            req.set_block_uuid(block_uuid);
            
            StatusResponse resp;
            grpc::ClientContext ctx;
            
            auto status = node->stub->DeleteBlock(&ctx, req, &resp);
            if (!status.ok()) {
                std::cerr << "Failed to delete block " << block_uuid << " from node " 
                          << node->address << ": " << status.error_message() << std::endl;
            } else {
                std::cout << "Deleted block " << block_uuid << " from node " 
                          << node->address << std::endl;
            }
        }
    }
    
    // 5. Remove file from inode table
    root_inode.children.erase(child_it);
    free_inodes.push(file_id);
    
    std::cout << "Deleted file at " << path << " (inode " << file_id 
              << ") for user " << user_id << std::endl;
    
    response->set_success(true);
    response->set_error("");
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
