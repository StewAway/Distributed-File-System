#include "fs_server/fsserver_service.hpp"
#include <iostream>
#include <memory>
#include <signal.h>
#include <thread>
#include <chrono>

static std::unique_ptr<grpc::Server> g_server;

void SignalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        std::cout << "\nShutting down server..." << std::endl;
        if (g_server) {
            g_server->Shutdown();
        }
    }
}

int main(int argc, char* argv[]) {
    // Parse command line arguments
    std::string datanode_id = "datanode-1";
    std::string blocks_dir = "./blocks";
    std::string server_address = "0.0.0.0:50051";
    bool cache_enabled = false;
    uint64_t cache_size = 4096; // 4096 pages default cache size
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) {
            datanode_id = argv[++i];
        } else if (arg == "--blocks" && i + 1 < argc) {
            blocks_dir = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            server_address = "0.0.0.0:" + std::string(argv[++i]);
        } else if (arg == "--cache-enable" && i + 1 < argc) {
            cache_enabled = std::string(argv[++i]) == "true";
        } else if (arg == "--cache-size" && i + 1 < argc) {
            cache_size = std::stoull(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: fs_server [options]" << std::endl
                     << "Options:" << std::endl
                     << "  --id <id>         Datanode identifier (default: datanode-1)" << std::endl
                     << "  --blocks <path>   Blocks directory (default: ./blocks)" << std::endl
                     << "  --port <port>     Server port (default: 50051)" << std::endl
                     << "  --cache-enable <true|false> Enable or disable cache (default: false)" << std::endl
                     << "  --cache-size <Pages> Cache size in pages (default: 4096)" << std::endl
                     << "  --help            Show this help message" << std::endl;
            return 0;
        }
    }
    
    std::cout << "================================" << std::endl
             << "  Distributed File System" << std::endl
             << "  Data Node (FSServer)" << std::endl
             << "================================" << std::endl
             << "Datanode ID: " << datanode_id << std::endl
             << "Blocks Dir: " << blocks_dir << std::endl
             << "Server Address: " << server_address << std::endl
             << "Cache Enabled: " << (cache_enabled ? "true" : "false") << std::endl
             << "Cache Size (Number of Pages): " << cache_size << std::endl
             << std::endl;
    
    // Create service implementation
    auto service = std::make_unique<fs_server::FSServerServiceImpl>(
        datanode_id, blocks_dir, cache_enabled, cache_size);
    
    // Build and start server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    
    g_server = builder.BuildAndStart();
    
    if (!g_server) {
        std::cerr << "Failed to start server!" << std::endl;
        return 1;
    }
    
    std::cout << "FSServer listening on " << server_address << std::endl;
    std::cout << "Press Ctrl+C to shutdown..." << std::endl << std::endl;
    
    // Setup signal handlers
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    
    // Print statistics periodically (for monitoring)
    std::thread stats_thread([&service]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            std::cout << "\n" << service->GetStatistics() << std::endl;
        }
    });
    stats_thread.detach();
    
    // Wait for server shutdown
    g_server->Wait();
    
    std::cout << "Server shutdown complete." << std::endl;
    
    return 0;
}
