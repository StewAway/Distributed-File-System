#include "fs_server/fsserver_service.hpp"
#include "fs_server/cache.hpp"
#include <iostream>
#include <memory>
#include <signal.h>
#include <thread>
#include <chrono>
#include <atomic>

static std::unique_ptr<grpc::Server> g_server;
static std::atomic<bool> g_shutdown_requested(false);

void SignalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        std::cout << "\nShutting down server..." << std::endl;
        g_shutdown_requested.store(true);
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
    fs_server::CachePolicy cache_policy = fs_server::CachePolicy::LRU; // Default to LRU
    
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
        } else if (arg == "--cache-policy" && i + 1 < argc) {
            std::string policy_str = argv[++i];
            if (policy_str == "lru" || policy_str == "LRU") {
                cache_policy = fs_server::CachePolicy::LRU;
            } else if (policy_str == "lfu" || policy_str == "LFU") {
                cache_policy = fs_server::CachePolicy::LFU;
            } else {
                std::cerr << "Unknown cache policy: " << policy_str << ". Using LRU." << std::endl;
            }
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: fs_server [options]" << std::endl
                     << "Options:" << std::endl
                     << "  --id <id>         Datanode identifier (default: datanode-1)" << std::endl
                     << "  --blocks <path>   Blocks directory (default: ./blocks)" << std::endl
                     << "  --port <port>     Server port (default: 50051)" << std::endl
                     << "  --cache-enable <true|false> Enable or disable cache (default: false)" << std::endl
                     << "  --cache-size <Pages> Cache size in pages (default: 4096)" << std::endl
                     << "  --cache-policy <lru|lfu> Cache eviction policy (default: lru)" << std::endl
                     << "  --help            Show this help message" << std::endl;
            return 0;
        }
    }
    
    std::string policy_name = (cache_policy == fs_server::CachePolicy::LRU) ? "LRU" : "LFU";
    
    std::cout << "================================" << std::endl
             << "  Distributed File System" << std::endl
             << "  Data Node (FSServer)" << std::endl
             << "================================" << std::endl
             << "Datanode ID: " << datanode_id << std::endl
             << "Blocks Dir: " << blocks_dir << std::endl
             << "Server Address: " << server_address << std::endl
             << "Cache Enabled: " << (cache_enabled ? "true" : "false") << std::endl
             << "Cache Size (Number of Pages): " << cache_size << std::endl
             << "Cache Policy: " << policy_name << std::endl
             << std::endl;
    
    // Create service implementation
    auto service = std::make_unique<fs_server::FSServerServiceImpl>(
        datanode_id, blocks_dir, cache_enabled, cache_size, cache_policy);
    
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
        while (!g_shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            if (!g_shutdown_requested.load()) {
                std::cout << "\n" << service->GetStatistics() << std::endl;
            }
        }
    });
    stats_thread.detach();
    
    // Background dirty page flusher thread (only when cache is enabled)
    // Flushes all dirty pages when num_dirty_pages exceeds 40% of cache capacity
    constexpr double DIRTY_PAGE_THRESHOLD_RATIO = 0.4;  // 40% threshold
    constexpr int FLUSHER_CHECK_INTERVAL_MS = 100;      // Check every 100ms
    
    std::thread flusher_thread;
    if (cache_enabled) {
        uint64_t dirty_threshold = static_cast<uint64_t>(
            cache_size * DIRTY_PAGE_THRESHOLD_RATIO);
        
        std::cout << "Background dirty page flusher enabled (threshold: " 
                  << dirty_threshold << " pages, " 
                  << static_cast<int>(DIRTY_PAGE_THRESHOLD_RATIO * 100) << "% of cache)"
                  << std::endl;
        
        flusher_thread = std::thread([&service, dirty_threshold, FLUSHER_CHECK_INTERVAL_MS]() {
            while (!g_shutdown_requested.load()) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(FLUSHER_CHECK_INTERVAL_MS));
                
                if (g_shutdown_requested.load()) {
                    break;
                }
                
                uint64_t dirty_count = service->GetDirtyPageCount();
                if (dirty_count >= dirty_threshold) {
                    std::cout << "Background flusher: Dirty page count (" << dirty_count 
                              << ") exceeded threshold (" << dirty_threshold 
                              << "), flushing..." << std::endl;
                    
                    uint64_t flushed = service->FlushDirtyPages();
                    
                    std::cout << "Background flusher: Flushed " << flushed 
                              << " dirty pages to disk" << std::endl;
                }
            }
            std::cout << "Background flusher thread stopped." << std::endl;
        });
    }
    
    // Wait for server shutdown
    g_server->Wait();
    
    // Signal threads to stop
    g_shutdown_requested.store(true);
    
    // Wait for flusher thread to finish if it was started
    if (flusher_thread.joinable()) {
        flusher_thread.join();
    }
    
    std::cout << "Server shutdown complete." << std::endl;
    
    return 0;
}
