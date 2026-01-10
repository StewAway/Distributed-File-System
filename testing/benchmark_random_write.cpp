/**
 * Benchmark: Random Write Performance Test
 * 
 * Purpose: Measure the write performance of the distributed file system
 * with random write patterns to stress test the page cache.
 * 
 * Test Scenario:
 * - Creates files and writes data at random offsets
 * - Simulates real-world random access patterns
 * - Tests cache write-back behavior under random access
 * 
 * Setup: 
 * - 1 fs_master running on localhost:50050
 * - 3 fs_server instances running on localhost:50051, 50052, 50053
 */

#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <fstream>
#include <algorithm>
#include <grpcpp/grpcpp.h>
#include "fs_service/fs.grpc.pb.h"

// Color codes for output
#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define CYAN "\033[36m"
#define RESET "\033[0m"

// Benchmark configuration
struct BenchmarkConfig {
    std::string master_addr = "localhost:50050";
    std::string user_id = "benchmark_user";
    
    // Test parameters
    uint64_t num_files = 10;              // Number of files to create
    uint64_t file_size_kb = 1024;         // Size of each file in KB (1MB default)
    uint64_t chunk_size_kb = 64;          // Write chunk size in KB (64KB = 1 block)
    uint64_t num_random_writes = 100;     // Number of random write operations
    uint64_t random_seed = 42;            // Random seed for reproducibility
    bool verbose = false;                  // Print detailed output
};

// Statistics collector
struct BenchmarkStats {
    std::string test_name;
    uint64_t num_operations = 0;
    uint64_t total_bytes = 0;
    uint64_t successful_ops = 0;
    uint64_t failed_ops = 0;
    std::chrono::duration<double> total_time{0};
    std::vector<double> latencies_ms;
    
    // Track unique blocks accessed for cache analysis
    uint64_t unique_files_accessed = 0;
    uint64_t unique_offsets_accessed = 0;
    
    double GetThroughputMBps() const {
        if (total_time.count() == 0) return 0;
        return (total_bytes / (1024.0 * 1024.0)) / total_time.count();
    }
    
    double GetOpsPerSecond() const {
        if (total_time.count() == 0) return 0;
        return num_operations / total_time.count();
    }
    
    double GetAvgLatencyMs() const {
        if (latencies_ms.empty()) return 0;
        double sum = 0;
        for (double lat : latencies_ms) sum += lat;
        return sum / latencies_ms.size();
    }
    
    double GetP99LatencyMs() const {
        if (latencies_ms.empty()) return 0;
        std::vector<double> sorted = latencies_ms;
        std::sort(sorted.begin(), sorted.end());
        size_t idx = static_cast<size_t>(sorted.size() * 0.99);
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted[idx];
    }
    
    double GetP50LatencyMs() const {
        if (latencies_ms.empty()) return 0;
        std::vector<double> sorted = latencies_ms;
        std::sort(sorted.begin(), sorted.end());
        return sorted[sorted.size() / 2];
    }
    
    void Print() const {
        std::cout << "\n" << CYAN << std::string(70, '=') << RESET << std::endl;
        std::cout << CYAN << "Benchmark: " << test_name << RESET << std::endl;
        std::cout << CYAN << std::string(70, '=') << RESET << std::endl;
        std::cout << "Total Operations:  " << num_operations << std::endl;
        std::cout << "Successful Ops:    " << GREEN << successful_ops << RESET << std::endl;
        std::cout << "Failed Ops:        " << (failed_ops > 0 ? RED : "") << failed_ops << RESET << std::endl;
        std::cout << "Total Bytes:       " << total_bytes << " (" 
                  << std::fixed << std::setprecision(2) 
                  << (total_bytes / (1024.0 * 1024.0)) << " MB)" << std::endl;
        std::cout << "Total Time:        " << std::fixed << std::setprecision(3) 
                  << total_time.count() << " seconds" << std::endl;
        std::cout << YELLOW << "Throughput:        " << std::fixed << std::setprecision(2) 
                  << GetThroughputMBps() << " MB/s" << RESET << std::endl;
        std::cout << "Ops/sec:           " << std::fixed << std::setprecision(0) 
                  << GetOpsPerSecond() << std::endl;
        std::cout << "Avg Latency:       " << std::fixed << std::setprecision(2) 
                  << GetAvgLatencyMs() << " ms" << std::endl;
        std::cout << "P50 Latency:       " << std::fixed << std::setprecision(2) 
                  << GetP50LatencyMs() << " ms" << std::endl;
        std::cout << "P99 Latency:       " << std::fixed << std::setprecision(2) 
                  << GetP99LatencyMs() << " ms" << std::endl;
        std::cout << "\n" << YELLOW << "Random Access Pattern Analysis:" << RESET << std::endl;
        std::cout << "  Unique Files:    " << unique_files_accessed << std::endl;
        std::cout << "  Unique Offsets:  " << unique_offsets_accessed << std::endl;
        std::cout << CYAN << std::string(70, '=') << RESET << std::endl;
    }
    
    void SaveToCSV(const std::string& filename) const {
        std::ofstream file(filename, std::ios::app);
        if (file.is_open()) {
            file << test_name << ","
                 << num_operations << ","
                 << total_bytes << ","
                 << successful_ops << ","
                 << failed_ops << ","
                 << total_time.count() << ","
                 << GetThroughputMBps() << ","
                 << GetOpsPerSecond() << ","
                 << GetAvgLatencyMs() << ","
                 << GetP99LatencyMs() << "\n";
            file.close();
        }
    }
};

class RandomWriteBenchmark {
private:
    std::unique_ptr<FSMasterService::Stub> stub_;
    BenchmarkConfig config_;
    BenchmarkStats stats_;
    std::mt19937_64 rng_;
    
    // Track file descriptors
    std::vector<int> file_fds_;
    std::vector<std::string> file_paths_;

public:
    RandomWriteBenchmark(const BenchmarkConfig& config) 
        : config_(config), rng_(config.random_seed) {
        auto channel = grpc::CreateChannel(
            config_.master_addr,
            grpc::InsecureChannelCredentials()
        );
        stub_ = FSMasterService::NewStub(channel);
        stats_.test_name = "Random Write";
    }

    bool Mount() {
        MountRequest request;
        request.set_user_id(config_.user_id);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->Mount(&context, request, &response);
        if (!status.ok() || !response.success()) {
            std::cerr << RED << "Failed to mount: " << response.error() << RESET << std::endl;
            return false;
        }
        std::cout << GREEN << "Mounted user: " << config_.user_id << RESET << std::endl;
        return true;
    }

    bool UnMount() {
        MountRequest request;
        request.set_user_id(config_.user_id);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->UnMount(&context, request, &response);
        return status.ok() && response.success();
    }

    int OpenFile(const std::string& path, const std::string& mode) {
        OpenRequest request;
        request.set_user_id(config_.user_id);
        request.set_path(path);
        request.set_mode(mode);
        
        OpenResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->Open(&context, request, &response);
        if (!status.ok() || response.fd() <= 0) {
            if (config_.verbose) {
                std::cerr << RED << "Failed to open " << path << ": " 
                          << response.error() << RESET << std::endl;
            }
            return -1;
        }
        return response.fd();
    }

    bool WriteFile(int fd, const std::string& data, uint64_t offset = 0) {
        WriteRequest request;
        request.set_user_id(config_.user_id);
        request.set_fd(fd);
        request.set_offset(offset);
        request.set_data(data);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->Write(&context, request, &response);
        return status.ok() && response.success();
    }

    bool CloseFile(int fd) {
        CloseRequest request;
        request.set_user_id(config_.user_id);
        request.set_fd(fd);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->Close(&context, request, &response);
        return status.ok() && response.success();
    }

    bool DeleteFile(const std::string& path) {
        DeleteFileRequest request;
        request.set_user_id(config_.user_id);
        request.set_path(path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->DeleteFile(&context, request, &response);
        return status.ok() && response.success();
    }

    std::string GenerateData(uint64_t size) {
        std::string data(size, 0);
        std::uniform_int_distribution<int> dist(0, 255);
        for (uint64_t i = 0; i < size; ++i) {
            data[i] = static_cast<char>(dist(rng_));
        }
        return data;
    }

    bool SetupTestFiles() {
        std::cout << "\nSetting up test files..." << std::endl;
        
        // Pre-allocate files with initial data
        std::string initial_data = GenerateData(config_.file_size_kb * 1024);

        for (uint64_t file_idx = 0; file_idx < config_.num_files; ++file_idx) {
            std::string filepath = "/benchmark/rand_write_" + std::to_string(file_idx) + ".dat";
            file_paths_.push_back(filepath);
            
            int fd = OpenFile(filepath, "w");
            if (fd < 0) {
                std::cerr << RED << "Failed to create test file: " << filepath << RESET << std::endl;
                return false;
            }

            // Write initial data to pre-allocate the file
            if (!WriteFile(fd, initial_data, 0)) {
                std::cerr << RED << "Failed to write initial data" << RESET << std::endl;
                CloseFile(fd);
                return false;
            }

            CloseFile(fd);
            
            if ((file_idx + 1) % 5 == 0 || file_idx == config_.num_files - 1) {
                std::cout << "\rSetup progress: " << (file_idx + 1) << "/" << config_.num_files 
                          << " files created" << std::flush;
            }
        }
        std::cout << std::endl;
        
        // Now open all files for random access
        std::cout << "Opening files for random access..." << std::endl;
        for (const auto& path : file_paths_) {
            int fd = OpenFile(path, "w");
            if (fd < 0) {
                std::cerr << RED << "Failed to open file for random access: " << path << RESET << std::endl;
                return false;
            }
            file_fds_.push_back(fd);
        }
        
        return true;
    }

    void RunBenchmark() {
        std::cout << "\n" << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "Random Write Benchmark" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << "Files: " << config_.num_files << std::endl;
        std::cout << "File Size: " << config_.file_size_kb << " KB" << std::endl;
        std::cout << "Chunk Size: " << config_.chunk_size_kb << " KB" << std::endl;
        std::cout << "Random Writes: " << config_.num_random_writes << std::endl;
        std::cout << "Random Seed: " << config_.random_seed << std::endl;

        if (!Mount()) {
            std::cerr << RED << "Failed to mount, aborting benchmark" << RESET << std::endl;
            return;
        }

        // Setup test files first
        if (!SetupTestFiles()) {
            std::cerr << RED << "Failed to setup test files, aborting" << RESET << std::endl;
            UnMount();
            return;
        }

        std::cout << "\nStarting random write benchmark..." << std::endl;
        
        // Pre-generate write data
        std::string chunk_data = GenerateData(config_.chunk_size_kb * 1024);
        
        // Calculate valid offset range (aligned to chunk size)
        uint64_t max_offset = config_.file_size_kb - config_.chunk_size_kb;
        uint64_t num_valid_offsets = max_offset / config_.chunk_size_kb + 1;
        
        std::uniform_int_distribution<uint64_t> file_dist(0, config_.num_files - 1);
        std::uniform_int_distribution<uint64_t> offset_dist(0, num_valid_offsets - 1);
        
        // Track unique accesses
        std::set<uint64_t> unique_files;
        std::set<std::pair<uint64_t, uint64_t>> unique_file_offsets;

        auto benchmark_start = std::chrono::high_resolution_clock::now();

        for (uint64_t op = 0; op < config_.num_random_writes; ++op) {
            // Select random file and offset
            uint64_t file_idx = file_dist(rng_);
            uint64_t offset_idx = offset_dist(rng_);
            uint64_t offset = offset_idx * config_.chunk_size_kb * 1024;
            
            unique_files.insert(file_idx);
            unique_file_offsets.insert({file_idx, offset});
            
            int fd = file_fds_[file_idx];
            
            if (config_.verbose) {
                std::cout << "Writing to file " << file_idx << " at offset " << offset << std::endl;
            }

            auto op_start = std::chrono::high_resolution_clock::now();
            bool success = WriteFile(fd, chunk_data, offset);
            auto op_end = std::chrono::high_resolution_clock::now();
            
            stats_.num_operations++;
            if (success) {
                stats_.successful_ops++;
                stats_.total_bytes += chunk_data.size();
                
                double latency_ms = std::chrono::duration<double, std::milli>(
                    op_end - op_start).count();
                stats_.latencies_ms.push_back(latency_ms);
            } else {
                stats_.failed_ops++;
                if (config_.verbose) {
                    std::cerr << RED << "Write failed" << RESET << std::endl;
                }
            }

            // Progress indicator
            if ((op + 1) % 100 == 0 || op == config_.num_random_writes - 1) {
                std::cout << "\rProgress: " << (op + 1) << "/" << config_.num_random_writes 
                          << " writes completed" << std::flush;
            }
        }
        std::cout << std::endl;

        auto benchmark_end = std::chrono::high_resolution_clock::now();
        stats_.total_time = benchmark_end - benchmark_start;
        stats_.unique_files_accessed = unique_files.size();
        stats_.unique_offsets_accessed = unique_file_offsets.size();

        // Close all files
        for (int fd : file_fds_) {
            CloseFile(fd);
        }
        file_fds_.clear();

        // Print results
        stats_.Print();

        // Cleanup - delete test files
        std::cout << "\nCleaning up test files..." << std::endl;
        for (const auto& path : file_paths_) {
            DeleteFile(path);
        }

        UnMount();
    }

    const BenchmarkStats& GetStats() const { return stats_; }
};

void PrintUsage(const char* program) {
    std::cout << "Usage: " << program << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --master <addr>     Master address (default: localhost:50050)" << std::endl;
    std::cout << "  --files <n>         Number of files to create (default: 10)" << std::endl;
    std::cout << "  --file-size <kb>    Size of each file in KB (default: 1024)" << std::endl;
    std::cout << "  --chunk-size <kb>   Write chunk size in KB (default: 64)" << std::endl;
    std::cout << "  --writes <n>        Number of random writes (default: 100)" << std::endl;
    std::cout << "  --seed <n>          Random seed (default: 42)" << std::endl;
    std::cout << "  --verbose           Enable verbose output" << std::endl;
    std::cout << "  --csv <file>        Save results to CSV file" << std::endl;
    std::cout << "  --help              Show this help" << std::endl;
}

int main(int argc, char* argv[]) {
    BenchmarkConfig config;
    std::string csv_file;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            config.master_addr = argv[++i];
        } else if (arg == "--files" && i + 1 < argc) {
            config.num_files = std::stoull(argv[++i]);
        } else if (arg == "--file-size" && i + 1 < argc) {
            config.file_size_kb = std::stoull(argv[++i]);
        } else if (arg == "--chunk-size" && i + 1 < argc) {
            config.chunk_size_kb = std::stoull(argv[++i]);
        } else if (arg == "--writes" && i + 1 < argc) {
            config.num_random_writes = std::stoull(argv[++i]);
        } else if (arg == "--seed" && i + 1 < argc) {
            config.random_seed = std::stoull(argv[++i]);
        } else if (arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--csv" && i + 1 < argc) {
            csv_file = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        }
    }

    std::cout << CYAN << "╔══════════════════════════════════════════════════════════════╗" << RESET << std::endl;
    std::cout << CYAN << "║       Random Write Benchmark - Page Cache Performance        ║" << RESET << std::endl;
    std::cout << CYAN << "╚══════════════════════════════════════════════════════════════╝" << RESET << std::endl;

    RandomWriteBenchmark benchmark(config);
    benchmark.RunBenchmark();

    if (!csv_file.empty()) {
        benchmark.GetStats().SaveToCSV(csv_file);
        std::cout << "Results saved to: " << csv_file << std::endl;
    }

    return 0;
}
