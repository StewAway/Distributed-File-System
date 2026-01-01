#pragma once
#include "fs_service/fs.grpc.pb.h"
#include "fs_master/inode.hpp"
#include "fs_master/user_context.hpp"
#include <memory>
#include <vector>
#include <grpcpp/grpcpp.h>

namespace fs_master {

/**
 * DataNodeSelector: Manages replica selection for GFS/HDFS-like replication.
 * 
 * This class handles:
 * - Maintaining a list of available data nodes (FSServers)
 * - Selecting appropriate nodes for read/write operations based on replication factor
 * - Future: Handle fault tolerance and node failure detection
 * 
 * Usage:
 *   DataNodeSelector selector(replication_factor = 3);
 *   selector.RegisterDataNode("datanode1:50051", stub1);
 *   auto nodes = selector.SelectNodesForWrite(block_uuid);
 */
class DataNodeSelector {
public:
    struct DataNode {
        std::string address;
        std::shared_ptr<FSServerService::Stub> stub;
        bool is_healthy = true;
    };

    explicit DataNodeSelector(int replication_factor = 3);
    
    /**
     * Register a new data node (FSServer) with the master.
     * Called during cluster setup or when new nodes join.
     */
    void RegisterDataNode(const std::string& address, 
                          std::shared_ptr<FSServerService::Stub> stub);
    
    /**
     * Select nodes for writing a block.
     * Future: Implement rack-aware placement for fault tolerance.
     * Returns: vector of selected DataNodes based on replication factor
     */
    std::vector<DataNode*> SelectNodesForWrite(uint64_t block_uuid);
    
    /**
     * Select a single node for reading a block.
     * Future: Implement load balancing across replicas.
     */
    DataNode* SelectNodeForRead(uint64_t block_uuid);

private:
    std::vector<DataNode> data_nodes_;
    int replication_factor_;
    size_t round_robin_index_ = 0;
};

/**
 * FSMasterServiceImpl: Main gRPC service implementation for the File System Master.
 * 
 * This service handles all client requests and coordinates with FSServers (data nodes).
 * Thread-safe operations are ensured through proper synchronization (future: add mutex).
 */
class FSMasterServiceImpl final : public FSMasterService::Service {
public:
    explicit FSMasterServiceImpl(std::shared_ptr<DataNodeSelector> selector);

    /**
     * Mount: Initialize user session on the file system.
     * Creates a new user context if not exists.
     */
    grpc::Status Mount(grpc::ServerContext*, const MountRequest*, StatusResponse*) override;

    /**
     * Open: Open a file for reading or writing.
     * Allocates file descriptor and creates inode if needed.
     * Args:
     *   - user_id: Unique user identifier
     *   - path: File path
     *   - mode: "r" (read), "w" (write), "rw" (read-write), "a" (append)
     */
    grpc::Status Open(grpc::ServerContext*, const OpenRequest*, OpenResponse*) override;

    /**
     * Read: Read bytes from an open file.
     * Locates blocks and fetches data from appropriate FSServers.
     */
    grpc::Status Read(grpc::ServerContext*, const ReadRequest*, ReadResponse*) override;

    /**
     * Write: Write bytes to an open file.
     * Allocates new blocks and replicates across FSServers based on replication factor.
     */
    grpc::Status Write(grpc::ServerContext*, const WriteRequest*, StatusResponse*) override;

    /**
     * Close: Close an open file descriptor.
     * Finalizes any pending operations.
     */
    grpc::Status Close(grpc::ServerContext*, const CloseRequest*, StatusResponse*) override;

    /**
     * Mkdir: Create a new directory.
     */
    grpc::Status Mkdir(grpc::ServerContext*, const MkdirRequest*, StatusResponse*) override;

    /**
     * Rmdir: Remove a directory (must be empty).
     */
    grpc::Status Rmdir(grpc::ServerContext*, const RmdirRequest*, StatusResponse*) override;

    /**
     * Ls: List directory contents.
     */
    grpc::Status Ls(grpc::ServerContext*, const LsRequest*, LsResponse*) override;

    /**
     * DeleteFile: Delete a file and free up blocks.
     */
    grpc::Status DeleteFile(grpc::ServerContext*, const DeleteFileRequest*, StatusResponse*) override;

private:
    std::shared_ptr<DataNodeSelector> data_node_selector_;
    
    /**
     * Internal helper: Navigate the inode tree to find parent and check permissions.
     * Returns: Parent inode ID, or 0 if path not found
     */
    uint64_t ResolvePath(const std::string& path, uint64_t user_root);
    
    /**
     * Internal helper: Call FSServer to write data to blocks.
     * This is where DataNodeSelector determines which nodes to use.
     * Example implementation:
     *   auto nodes = data_node_selector_->SelectNodesForWrite(block_uuid);
     *   for (auto node : nodes) {
     *       BlockRequest req;
     *       req.set_block_uuid(block_uuid);
     *       req.set_data(data);
     *       auto status = node->stub->WriteBlock(&context, req, &response);
     *   }
     */
    grpc::Status WriteBlockToFSServers(uint64_t block_uuid, const std::string& data);
    
    /**
     * Internal helper: Call FSServer to read data from blocks.
     * Uses DataNodeSelector to choose which replica to read from.
     */
    grpc::Status ReadBlockFromFSServer(uint64_t block_uuid, std::string& data);
};

}  // namespace fs_master