#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_impl.h>
#include <grpcpp/ext/proto_plus_reflection.h>
#include "fs_master/fsmaster_service.hpp"
#include "fs_master/inode.hpp"
#include "fs_master/user_context.hpp"

namespace fs_master {
// Global state for the filesystem master
std::unordered_map<std::string, UserContext> active_users;
std::unordered_map<std::string, uint64_t> user_roots;
std::unordered_map<uint64_t, Inode> inode_table;
}  // namespace fs_master

using grpc::Server;
using grpc::ServerBuilder;

// ============================================================================
// Configuration
// ============================================================================
const std::string DEFAULT_HOST = "0.0.0.0";
const int DEFAULT_PORT = 50050;
const int DEFAULT_REPLICATION_FACTOR = 3;

// ============================================================================
// Helper: Parse command-line arguments
// ============================================================================
struct ServerConfig {
    std::string host = DEFAULT_HOST;
    int port = DEFAULT_PORT;
    int replication_factor = DEFAULT_REPLICATION_FACTOR;
    std::vector<std::pair<std::string, int>> data_nodes;  // {address, port}
};

ServerConfig ParseArgs(int argc, char* argv[]) {
    ServerConfig config;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--port" && i + 1 < argc) {
            config.port = std::stoi(argv[++i]);
        } else if (arg == "--host" && i + 1 < argc) {
            config.host = argv[++i];
        } else if (arg == "--replication" && i + 1 < argc) {
            config.replication_factor = std::stoi(argv[++i]);
        } else if (arg == "--datanode" && i + 1 < argc) {
            // Format: "host:port"
            std::string datanode = argv[++i];
            size_t colon_pos = datanode.find(':');
            if (colon_pos != std::string::npos) {
                std::string host = datanode.substr(0, colon_pos);
                int port = std::stoi(datanode.substr(colon_pos + 1));
                config.data_nodes.push_back({host, port});
            }
        }
    }
    
    return config;
}

// ============================================================================
// Helper: Initialize data nodes (FSServers)
// ============================================================================
void InitializeDataNodes(std::shared_ptr<fs_master::DataNodeSelector> selector,
                         const std::vector<std::pair<std::string, int>>& data_nodes) {
    for (const auto& [host, port] : data_nodes) {
        std::string target = host + ":" + std::to_string(port);
        
        std::cout << "Connecting to data node: " << target << std::endl;
        
        // Create a channel to the data node
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        
        // Create a stub for the FSServerService
        auto stub = fs_master::FSServerService::NewStub(channel);
        
        // Register with the selector
        selector->RegisterDataNode(target, stub);
    }
}

// ============================================================================
// Helper: Print server configuration and status
// ============================================================================
void PrintServerInfo(const ServerConfig& config) {
    std::cout << "========================================" << std::endl;
    std::cout << "  FS Master Server Starting" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Host: " << config.host << std::endl;
    std::cout << "Port: " << config.port << std::endl;
    std::cout << "Replication Factor: " << config.replication_factor << std::endl;
    std::cout << "Connected Data Nodes: " << config.data_nodes.size() << std::endl;
    for (size_t i = 0; i < config.data_nodes.size(); ++i) {
        std::cout << "  [" << i + 1 << "] " << config.data_nodes[i].first 
                  << ":" << config.data_nodes[i].second << std::endl;
    }
    std::cout << "========================================" << std::endl;
}

// ============================================================================
// Main: gRPC Server Initialization and Startup
// ============================================================================
int main(int argc, char* argv[]) {
    // Parse configuration from command-line arguments
    ServerConfig config = ParseArgs(argc, argv);
    PrintServerInfo(config);

    // ========================================================================
    // 1. Create DataNodeSelector (manages replica placement strategy)
    // ========================================================================
    auto data_node_selector = std::make_shared<fs_master::DataNodeSelector>(
        config.replication_factor);

    // ========================================================================
    // 2. Initialize connections to data nodes (FSServers)
    // ========================================================================
    // NOTE: In production with Docker:
    //   - Data nodes are discovered via service discovery (Kubernetes, Consul, etc.)
    //   - Or passed as environment variables from docker-compose
    //   - Example: docker run -e DATA_NODES="datanode1:50051,datanode2:50052"
    InitializeDataNodes(data_node_selector, config.data_nodes);

    // ========================================================================
    // 3. Create the FSMasterService implementation
    // ========================================================================
    auto service = std::make_unique<fs_master::FSMasterServiceImpl>(data_node_selector);

    // ========================================================================
    // 4. Setup gRPC Server with Health Check
    // ========================================================================
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    
    ServerBuilder builder;
    
    // Bind to the specified host and port
    std::string server_address = config.host + ":" + std::to_string(config.port);
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    
    // Register the service
    builder.RegisterService(service.get());
    
    // ========================================================================
    // 5. Build and start the server
    // ========================================================================
    std::unique_ptr<Server> server(builder.BuildAndStart());
    
    if (!server) {
        std::cerr << "Failed to build and start gRPC server!" << std::endl;
        return 1;
    }

    std::cout << std::endl;
    std::cout << "✓ gRPC Server listening on " << server_address << std::endl;
    std::cout << "✓ Ready to accept client connections..." << std::endl;
    std::cout << std::endl;

    // ========================================================================
    // 6. Wait for server shutdown (blocking call)
    // ========================================================================
    // In production with Docker:
    //   - Server will receive SIGTERM from container orchestration
    //   - Graceful shutdown will clean up resources
    server->Wait();

    std::cout << "gRPC Server shutdown gracefully." << std::endl;
    return 0;
}
