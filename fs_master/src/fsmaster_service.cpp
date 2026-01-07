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
    if (fs_master::UserExists(user_id)) {
        response->set_success(true);
        response->set_error("User already mounted");
        std::cout << "User " << user_id << " already mounted" << std::endl;
        return grpc::Status::OK;
    }
    
    // Create new user context
    fs_master::PutUserContext(user_id, UserContext());
    
    // Allocate root inode for this user (directory)
    uint64_t root_id = fs_master::allocate_inode_id();
    fs_master::PutInode(root_id, fs_master::Inode(root_id, true));  // is_directory = true
    fs_master::SetUserRoot(user_id, root_id);
    
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
    if (!fs_master::UserExists(user_id)) {
        response->set_success(false);
        response->set_error("User not mounted");
        std::cout << "User " << user_id << " not mounted" << std::endl;
        return grpc::Status::OK;
    }
    
    // Clean up user context
    fs_master::RemoveUser(user_id);
    
    // Optionally: Free up user's inodes and blocks
    // 1) add new user root id into free inode pool
    auto user_root_opt = fs_master::GetUserRoot(user_id);
    if (user_root_opt.has_value()) {
        fs_master::free_inodes.push(user_root_opt.value());
    }
    // 2) recursively clear up inodes and blocks
    // pending: implement recursive inode deletion
    
    std::cout << "User " << user_id << " unmounted" << std::endl;
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

std::vector<std::string> split_path(const std::string& path) {
    std::vector<std::string> components;
    size_t start = 0;
    size_t end = path.find('/');
    
    while (end != std::string::npos) {
        if (end != start) {
            components.push_back(path.substr(start, end - start));
        }
        start = end + 1;
        end = path.find('/', start);
    }
    
    if (start < path.length()) {
        components.push_back(path.substr(start));
    }
    
    return components;
}

/**
 * ResolvePath: Helper function to resolve/create paths in the inode tree.
 * 
 * Modes:
 *   - "check": Only check if path exists, return inode_id if found, else -1
 *   - "create": Recursively create directories if they don't exist, return final inode_id
 *   - "create_file": Same as create but creates a FILE inode at the end, return inode_id
 * 
 * Returns: inode_id of the resolved/created path, or -1 on failure
 */
int64_t ResolvePath(const std::string& path, const std::string& mode, uint64_t user_root,
    std::string& error_msg) {
    
    std::vector<std::string> components = split_path(path);
    
    if (components.empty()) {
        // Path is root
        return user_root;
    }
    
    uint64_t current_inode_id = user_root;
    
    for (size_t i = 0; i < components.size(); ++i) {
        const auto& component = components[i];
        
        // Check if current inode exists
        if (!fs_master::InodeExists(current_inode_id)) {
            error_msg = "Inode not found during path traversal";
            return -1;
        }
        
        auto current_inode_opt = fs_master::GetInode(current_inode_id);
        if (!current_inode_opt.has_value()) {
            error_msg = "Failed to retrieve current inode";
            return -1;
        }
        
        auto current_inode = current_inode_opt.value();
        
        if (!current_inode.is_directory) {
            error_msg = "Path component is not a directory: " + component;
            return -1;
        }
        
        bool is_last = (i == components.size() - 1);
        
        if (current_inode.children.find(component) == current_inode.children.end()) {
            // Component doesn't exist
            if (mode == "check") {
                error_msg = "Path not found: " + path;
                return -1;
            } else if (mode == "create" || mode == "create_file") {
                // Create new inode
                uint64_t new_inode_id = fs_master::allocate_inode_id();
                
                // Determine if this should be a directory or file
                bool is_dir = true;
                if (mode == "create_file" && is_last) {
                    is_dir = false;  // Last component is a file
                }
                
                // Create new inode
                current_inode.children[component] = new_inode_id;
                fs_master::PutInode(current_inode_id, current_inode);  // Update parent with new child reference
                fs_master::PutInode(new_inode_id, fs_master::Inode(new_inode_id, is_dir));
                
                std::cout << "Created " << (is_dir ? "directory" : "file") 
                          << " inode " << new_inode_id << " for path component: " << component << std::endl;
                
                current_inode_id = new_inode_id;
            } else {
                error_msg = "Unknown mode: " + mode;
                return -1;
            }
        } else {
            // Component exists
            uint64_t child_id = current_inode.children[component];
            
            // Check if child exists
            if (!fs_master::InodeExists(child_id)) {
                error_msg = "Child inode not found: " + component;
                return -1;
            }
            
            auto child_inode_opt = fs_master::GetInode(child_id);
            if (!child_inode_opt.has_value()) {
                error_msg = "Failed to retrieve child inode";
                return -1;
            }
            
            auto child_inode = child_inode_opt.value();
            
            // If mode is "create_file" and this is the last component, child must be a file
            if (mode == "create_file" && is_last && child_inode.is_directory) {
                error_msg = "Path exists but is a directory, expected file: " + path;
                return -1;
            }
            
            // If not last component, child must be a directory
            if (!is_last && !child_inode.is_directory) {
                error_msg = "Path component is not a directory: " + component;
                return -1;
            }
            
            current_inode_id = child_id;
        }
    }
    
    return current_inode_id;
}

grpc::Status FSMasterServiceImpl::Open(
    grpc::ServerContext* context,
    const OpenRequest* request,
    OpenResponse* response) {
    
    const std::string& user_id = request->user_id();
    const std::string& path = request->path();
    const std::string& mode = request->mode();
    
    // 1. Validate user is mounted and get context/root atomically
    auto user_and_root_opt = fs_master::GetUserContextAndRoot(user_id);
    if (!user_and_root_opt.has_value()) {
        response->set_fd(-1);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto& user_ctx = user_and_root_opt.value().context;
    uint64_t user_root = user_and_root_opt.value().root_id;
    
    // 2. Resolve path based on mode
    std::string error_msg;
    int64_t inode_id = -1;
    
    if (mode == "r" || mode == "rw") {
        // For read modes, only check if file exists
        inode_id = ResolvePath(path, "check", user_root, error_msg);
        if (inode_id == -1) {
            response->set_fd(-1);
            response->set_error("File not found for reading: " + error_msg);
            return grpc::Status::OK;
        }
        
        // Verify it's a file, not a directory
        auto inode_opt = fs_master::GetInode(inode_id);
        if (!inode_opt.has_value() || inode_opt.value().is_directory) {
            response->set_fd(-1);
            response->set_error("Cannot open directory as file: " + path);
            return grpc::Status::OK;
        }
    } else if (mode == "w") {
        // For write mode, create file and all parent directories if needed
        inode_id = ResolvePath(path, "create_file", user_root, error_msg);
        if (inode_id == -1) {
            response->set_fd(-1);
            response->set_error("Failed to create file: " + error_msg);
            return grpc::Status::OK;
        }
        
        // Truncate file if it already exists
        auto inode_opt = fs_master::GetInode(inode_id);
        if (inode_opt.has_value()) {
            auto inode = inode_opt.value();
            inode.blocks.clear();
            inode.size = 0;
            fs_master::PutInode(inode_id, inode);
        }
        std::cout << "Opened file for writing (truncated): " << path << std::endl;
    } else if (mode == "a") {
        // For append mode, create file if needed but don't truncate
        inode_id = ResolvePath(path, "create_file", user_root, error_msg);
        if (inode_id == -1) {
            response->set_fd(-1);
            response->set_error("Failed to open file for append: " + error_msg);
            return grpc::Status::OK;
        }
        // Don't truncate for append mode
        std::cout << "Opened file for appending: " << path << std::endl;
    } else {
        response->set_fd(-1);
        response->set_error("Invalid mode: " + mode);
        return grpc::Status::OK;
    }
    
    // 3. Allocate file descriptor
    int fd = ++user_ctx.fd_counter;
    
    FileSession session;
    session.inode_id = inode_id;
    
    // Get the current file size for offset calculation
    auto inode_opt = fs_master::GetInode(inode_id);
    session.offset = (mode == "a" && inode_opt.has_value()) ? inode_opt.value().size : 0;
    session.mode = mode;
    
    // Update user context with new open file descriptor
    auto user_ctx_opt = fs_master::GetUserContext(user_id);
    if (user_ctx_opt.has_value()) {
        auto user_ctx = user_ctx_opt.value();
        user_ctx.open_files[fd] = session;
        fs_master::PutUserContext(user_id, user_ctx);
    }
    
    std::cout << "Opened file at " << path << " with fd " << fd << " (inode " << inode_id << ")" << std::endl;
    
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
    auto user_ctx_opt = fs_master::GetUserContext(user_id);
    if (!user_ctx_opt.has_value()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "User not mounted");
    }
    
    auto user_ctx = user_ctx_opt.value();
    auto fd_it = user_ctx.open_files.find(fd);
    if (fd_it == user_ctx.open_files.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "File descriptor not found");
    }
    
    auto& session = fd_it->second;
    auto inode_opt = fs_master::GetInode(session.inode_id);
    if (!inode_opt.has_value()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Inode not found");
    }
    auto inode = inode_opt.value();
    
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
        
        // Call FSServerService::ReadBlockDataServer
        ReadBlockRequest req;
        req.set_block_uuid(block_uuid);
        
        ReadBlockResponse resp;
        grpc::ClientContext ctx;
        
        auto status = node->stub->ReadBlockDataServer(&ctx, req, &resp);
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
    auto user_ctx_opt = fs_master::GetUserContext(user_id);
    if (!user_ctx_opt.has_value()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto user_ctx = user_ctx_opt.value();
    auto fd_it = user_ctx.open_files.find(fd);
    if (fd_it == user_ctx.open_files.end()) {
        response->set_success(false);
        response->set_error("File descriptor not found");
        return grpc::Status::OK;
    }
    
    auto& session = fd_it->second;
    auto inode_opt = fs_master::GetInode(session.inode_id);
    if (!inode_opt.has_value()) {
        response->set_success(false);
        response->set_error("Inode not found");
        return grpc::Status::OK;
    }
    auto inode = inode_opt.value();
    
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
        uint64_t block_uuid = fs_master::allocate_block_uuid();
        
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
            
            auto status = node->stub->WriteBlockDataServer(&ctx, req, &resp);
            
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
    
    // 7. FINALIZE WRITE - Update inode with all blocks and new size
    inode.size += data.length();
    fs_master::PutInode(session.inode_id, inode);
    session.offset += data.length();
    
    // Update user context with modified session
    user_ctx.open_files[fd] = session;
    fs_master::PutUserContext(user_id, user_ctx);
    
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
    int32_t fd = request->fd();
    
    // Validate user is mounted
    auto user_ctx_opt = fs_master::GetUserContext(user_id);
    if (!user_ctx_opt.has_value()) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto user_ctx = user_ctx_opt.value();
    
    // Find and remove file descriptor by fd
    auto fd_it = user_ctx.open_files.find(fd);
    if (fd_it == user_ctx.open_files.end()) {
        response->set_success(false);
        response->set_error("File descriptor not open");
        return grpc::Status::OK;
    }
    
    user_ctx.open_files.erase(fd_it);
    fs_master::PutUserContext(user_id, user_ctx);
    
    std::cout << "Closed file descriptor " << fd << " for user " << user_id << std::endl;
    
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
    if (!fs_master::UserExists(user_id)) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto user_root_opt = fs_master::GetUserRoot(user_id);
    if (!user_root_opt.has_value()) {
        response->set_success(false);
        response->set_error("User root not found");
        return grpc::Status::OK;
    }
    uint64_t user_root = user_root_opt.value();
    
    // 2. Resolve path and create directory recursively
    std::string error_msg;
    int64_t inode_id = ResolvePath(path, "create", user_root, error_msg);
    
    if (inode_id == -1) {
        response->set_success(false);
        response->set_error("Failed to create directory: " + error_msg);
        return grpc::Status::OK;
    }
    
    // Verify the final inode is a directory
    auto inode_opt = fs_master::GetInode(inode_id);
    if (!inode_opt.has_value() || !inode_opt.value().is_directory) {
        response->set_success(false);
        response->set_error("Path exists but is not a directory: " + path);
        return grpc::Status::OK;
    }
    
    std::cout << "Created directory at " << path << " with inode " << inode_id 
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
    if (!fs_master::UserExists(user_id)) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto user_root_opt = fs_master::GetUserRoot(user_id);
    if (!user_root_opt.has_value()) {
        response->set_success(false);
        response->set_error("User root not found");
        return grpc::Status::OK;
    }
    uint64_t user_root = user_root_opt.value();
    
    // 2. Resolve path to find directory inode
    std::string error_msg;
    int64_t inode_id = ResolvePath(path, "check", user_root, error_msg);
    
    if (inode_id == -1) {
        response->set_success(false);
        response->set_error("Directory not found: " + error_msg);
        return grpc::Status::OK;
    }
    
    // 3. Verify it's a directory
    auto inode_opt = fs_master::GetInode(inode_id);
    if (!inode_opt.has_value() || !inode_opt.value().is_directory) {
        response->set_success(false);
        response->set_error("Not a directory: " + path);
        return grpc::Status::OK;
    }
    
    // 4. Check if directory is empty
    auto inode = inode_opt.value();
    if (!inode.children.empty()) {
        response->set_success(false);
        response->set_error("Directory not empty: " + path);
        return grpc::Status::OK;
    }
    
    // 5. Remove directory from parent
    std::vector<std::string> components = split_path(path);
    if (components.empty()) {
        response->set_success(false);
        response->set_error("Cannot remove root directory");
        return grpc::Status::OK;
    }
    
    const auto& dir_name = components.back();
    uint64_t parent_inode_id;
    if (components.size() == 1) {
        parent_inode_id = user_root;
    } else {
        std::string parent_path = "";
        for (size_t i = 0; i < components.size() - 1; ++i) {
            parent_path += "/" + components[i];
        }
        int64_t parent_id = ResolvePath(parent_path, "check", user_root, error_msg);
        if (parent_id == -1) {
            response->set_success(false);
            response->set_error("Parent directory not found");
            return grpc::Status::OK;
        }
        parent_inode_id = parent_id;
    }
    
    auto parent_inode_opt = fs_master::GetInode(parent_inode_id);
    if (parent_inode_opt.has_value()) {
        auto parent_inode = parent_inode_opt.value();
        parent_inode.children.erase(dir_name);
        fs_master::PutInode(parent_inode_id, parent_inode);
    }
    fs_master::free_inodes.push(inode_id);
    
    std::cout << "Removed directory at " << path << " (inode " << inode_id << ") for user " << user_id << std::endl;
    
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
    
    std::cout << "Listing directory for user: " << user_id << " path: " << path << std::endl;
    
    // 1. Validate user is mounted
    if (!fs_master::UserExists(user_id)) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "User not mounted");
    }
    
    auto user_root_opt = fs_master::GetUserRoot(user_id);
    if (!user_root_opt.has_value()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "User root not found");
    }
    uint64_t user_root = user_root_opt.value();
    
    // 2. Resolve path to find directory inode
    std::string error_msg;
    int64_t inode_id = ResolvePath(path, "check", user_root, error_msg);
    
    if (inode_id == -1) {
        response->set_success(false);
        response->set_error("Directory not found: " + error_msg);
        return grpc::Status::OK;
    }
    
    // 3. Verify it's a directory
    auto inode_opt = fs_master::GetInode(inode_id);
    if (!inode_opt.has_value() || !inode_opt.value().is_directory) {
        response->set_success(false);
        response->set_error("Not a directory: " + path);
        return grpc::Status::OK;
    }
    
    // 4. List directory contents
    const auto& inode = inode_opt.value();
    for (const auto& pair : inode.children) {
        const auto& name = pair.first;
        uint64_t child_id = pair.second;
        
        // Check if child_id exists using thread-safe accessor
        if (fs_master::InodeExists(child_id)) {
            auto child_inode_opt = fs_master::GetInode(child_id);
            if (child_inode_opt.has_value() && child_inode_opt.value().is_directory) {
                response->add_files(name + "/");
            } else {
                response->add_files(name);
            }
        } else {
            // Log warning but continue - corrupted inode reference
            std::cerr << "Warning: Child inode " << child_id << " not found for " << name << std::endl;
            response->add_files(name);  // Add without suffix if inode not found
        }
    }
    
    response->set_success(true);
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
    if (!fs_master::UserExists(user_id)) {
        response->set_success(false);
        response->set_error("User not mounted");
        return grpc::Status::OK;
    }
    
    auto user_root_opt = fs_master::GetUserRoot(user_id);
    if (!user_root_opt.has_value()) {
        response->set_success(false);
        response->set_error("User root not found");
        return grpc::Status::OK;
    }
    uint64_t user_root = user_root_opt.value();
    
    // 2. Resolve path to find file inode
    std::string error_msg;
    int64_t inode_id = ResolvePath(path, "check", user_root, error_msg);
    
    if (inode_id == -1) {
        response->set_success(false);
        response->set_error("File not found: " + error_msg);
        return grpc::Status::OK;
    }
    
    // 3. Verify it's a file, not a directory
    auto file_inode_opt = fs_master::GetInode(inode_id);
    if (!file_inode_opt.has_value() || file_inode_opt.value().is_directory) {
        response->set_success(false);
        response->set_error("Cannot delete directory with DeleteFile: " + path);
        return grpc::Status::OK;
    }
    auto file_inode = file_inode_opt.value();
    
    // 4. Delete blocks from all data nodes
    auto nodes = data_node_selector_->SelectNodesForWrite(0);
    
    for (const auto& block_uuid_str : file_inode.blocks) {
        uint64_t block_uuid = std::stoull(block_uuid_str);
        
        // Delete block from all replicas
        for (auto node : nodes) {
            DeleteBlockRequest req;
            req.set_block_uuid(block_uuid);
            
            StatusResponse resp;
            grpc::ClientContext ctx;
            
            auto status = node->stub->DeleteBlockDataServer(&ctx, req, &resp);
            if (!status.ok()) {
                std::cerr << "Failed to delete block " << block_uuid << " from node " 
                          << node->address << ": " << status.error_message() << std::endl;
            } else if (!resp.success()) {
                std::cerr << "Data node " << node->address << " failed to delete block " 
                          << block_uuid << ": " << resp.error() << std::endl;
            } else {
                std::cout << "Deleted block " << block_uuid << " from node " 
                          << node->address << std::endl;
            }
        }
    }
    
    // 5. Remove file from parent directory
    std::vector<std::string> components = split_path(path);
    const auto& filename = components.back();
    
    uint64_t parent_inode_id;
    if (components.size() == 1) {
        parent_inode_id = user_root;
    } else {
        std::string parent_path = "";
        for (size_t i = 0; i < components.size() - 1; ++i) {
            parent_path += "/" + components[i];
        }
        int64_t parent_id = ResolvePath(parent_path, "check", user_root, error_msg);
        if (parent_id == -1) {
            response->set_success(false);
            response->set_error("Parent directory not found");
            return grpc::Status::OK;
        }
        parent_inode_id = parent_id;
    }
    
    auto parent_inode_opt = fs_master::GetInode(parent_inode_id);
    if (parent_inode_opt.has_value()) {
        auto parent_inode = parent_inode_opt.value();
        parent_inode.children.erase(filename);
        fs_master::PutInode(parent_inode_id, parent_inode);
    }
    fs_master::free_inodes.push(inode_id);
    
    std::cout << "Deleted file at " << path << " (inode " << inode_id 
              << ") for user " << user_id << std::endl;
    
    response->set_success(true);
    response->set_error("");
    return grpc::Status::OK;
}

}  // namespace fs_master
