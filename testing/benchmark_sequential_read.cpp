/**
 * Benchmark: Sequential Read Performance Test
 * 
 * Purpose: Measure the read performance of the distributed file system
 * with sequential read patterns to stress test the page cache.
 * 
 * Test Scenario:
 * - First creates test files with known data
 * - Reads files sequentially and measures performance
 * - Measures cache hit rate by reading same files multiple times
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
#include <iomanip>
#include <fstream>
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
    uint64_t chunk_size_kb = 64;          // Read chunk size in KB (64KB = 1 block)
    uint64_t read_iterations = 3;         // Number of times to read all files
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
    
    // Per-iteration stats for cache analysis
    std::vector<double> iteration_throughputs;
    
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
        std::cout << "P99 Latency:       " << std::fixed << std::setprecision(2) 
                  << GetP99LatencyMs() << " ms" << std::endl;
        
        // Print per-iteration throughput to show cache warming
        if (!iteration_throughputs.empty()) {
            std::cout << "\n" << YELLOW << "Per-Iteration Throughput (Cache Effect):" << RESET << std::endl;
            for (size_t i = 0; i < iteration_throughputs.size(); ++i) {
                std::cout << "  Iteration " << (i + 1) << ": " 
                          << std::fixed << std::setprecision(2) 
                          << iteration_throughputs[i] << " MB/s";
                if (i > 0 && iteration_throughputs[0] > 0) {
                    double speedup = iteration_throughputs[i] / iteration_throughputs[0];
                    std::cout << " (" << std::setprecision(1) << speedup << "x vs first)";
                }
                std::cout << std::endl;
            }
        }
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

class SequentialReadBenchmark {
private:
    std::unique_ptr<FSMasterService::Stub> stub_;
    BenchmarkConfig config_;
    BenchmarkStats stats_;

public:
    SequentialReadBenchmark(const BenchmarkConfig& config) : config_(config) {
        auto channel = grpc::CreateChannel(
            config_.master_addr,
            grpc::InsecureChannelCredentials()
        );
        stub_ = FSMasterService::NewStub(channel);
        stats_.test_name = "Sequential Read";
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

    std::pair<bool, uint64_t> ReadFile(int fd, uint64_t count) {
        ReadRequest request;
        request.set_user_id(config_.user_id);
        request.set_fd(fd);
        request.set_count(count);
        
        ReadResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->Read(&context, request, &response);
        if (!status.ok()) {
            return {false, 0};
        }
        return {true, response.bytes_read()};
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
        for (uint64_t i = 0; i < size; ++i) {
            data[i] = static_cast<char>('A' + (i % 26));
        }
        return data;
    }

    bool SetupTestFiles() {
        std::cout << "\nSetting up test files..." << std::endl;
        
        std::string chunk_data = GenerateData(config_.chunk_size_kb * 1024);
        uint64_t chunks_per_file = config_.file_size_kb / config_.chunk_size_kb;

        for (uint64_t file_idx = 0; file_idx < config_.num_files; ++file_idx) {
            std::string filepath = "/benchmark/seq_read_" + std::to_string(file_idx) + ".dat";
            
            int fd = OpenFile(filepath, "w");
            if (fd < 0) {
                std::cerr << RED << "Failed to create test file: " << filepath << RESET << std::endl;
                return false;
            }

            for (uint64_t chunk_idx = 0; chunk_idx < chunks_per_file; ++chunk_idx) {
                uint64_t offset = chunk_idx * config_.chunk_size_kb * 1024;
                if (!WriteFile(fd, chunk_data, offset)) {
                    std::cerr << RED << "Failed to write test data" << RESET << std::endl;
                    CloseFile(fd);
                    return false;
                }
            }

            CloseFile(fd);
            
            if ((file_idx + 1) % 5 == 0 || file_idx == config_.num_files - 1) {
                std::cout << "\rSetup progress: " << (file_idx + 1) << "/" << config_.num_files 
                          << " files created" << std::flush;
            }
        }
        std::cout << std::endl;
        return true;
    }

    void RunBenchmark() {
        std::cout << "\n" << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "Sequential Read Benchmark" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << "Files: " << config_.num_files << std::endl;
        std::cout << "File Size: " << config_.file_size_kb << " KB" << std::endl;
        std::cout << "Chunk Size: " << config_.chunk_size_kb << " KB" << std::endl;
        std::cout << "Read Iterations: " << config_.read_iterations << std::endl;
        std::cout << "Total Data per Iteration: " << (config_.num_files * config_.file_size_kb / 1024.0) 
                  << " MB" << std::endl;

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

        std::cout << "\nStarting read benchmark..." << std::endl;
        
        uint64_t read_size = config_.chunk_size_kb * 1024;
        uint64_t reads_per_file = config_.file_size_kb / config_.chunk_size_kb;

        auto benchmark_start = std::chrono::high_resolution_clock::now();

        // Read all files multiple times to test cache effectiveness
        for (uint64_t iter = 0; iter < config_.read_iterations; ++iter) {
            std::cout << "\n" << YELLOW << "Iteration " << (iter + 1) << "/" 
                      << config_.read_iterations << RESET << std::endl;
            
            auto iter_start = std::chrono::high_resolution_clock::now();
            uint64_t iter_bytes = 0;

            for (uint64_t file_idx = 0; file_idx < config_.num_files; ++file_idx) {
                std::string filepath = "/benchmark/seq_read_" + std::to_string(file_idx) + ".dat";
                
                int fd = OpenFile(filepath, "r");
                if (fd < 0) {
                    stats_.failed_ops++;
                    continue;
                }

                // Read file sequentially in chunks
                for (uint64_t read_idx = 0; read_idx < reads_per_file; ++read_idx) {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    auto [success, bytes_read] = ReadFile(fd, read_size);
                    auto op_end = std::chrono::high_resolution_clock::now();
                    
                    stats_.num_operations++;
                    if (success && bytes_read > 0) {
                        stats_.successful_ops++;
                        stats_.total_bytes += bytes_read;
                        iter_bytes += bytes_read;
                        
                        double latency_ms = std::chrono::duration<double, std::milli>(
                            op_end - op_start).count();
                        stats_.latencies_ms.push_back(latency_ms);
                    } else {
                        stats_.failed_ops++;
                    }
                }

                CloseFile(fd);
            }

            auto iter_end = std::chrono::high_resolution_clock::now();
            double iter_time = std::chrono::duration<double>(iter_end - iter_start).count();
            double iter_throughput = (iter_bytes / (1024.0 * 1024.0)) / iter_time;
            stats_.iteration_throughputs.push_back(iter_throughput);
            
            std::cout << "  Throughput: " << std::fixed << std::setprecision(2) 
                      << iter_throughput << " MB/s" << std::endl;
        }

        auto benchmark_end = std::chrono::high_resolution_clock::now();
        stats_.total_time = benchmark_end - benchmark_start;

        // Print results
        stats_.Print();

        // Cleanup - delete test files
        std::cout << "\nCleaning up test files..." << std::endl;
        for (uint64_t file_idx = 0; file_idx < config_.num_files; ++file_idx) {
            std::string filepath = "/benchmark/seq_read_" + std::to_string(file_idx) + ".dat";
            DeleteFile(filepath);
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
    std::cout << "  --chunk-size <kb>   Read chunk size in KB (default: 64)" << std::endl;
    std::cout << "  --iterations <n>    Number of read iterations (default: 3)" << std::endl;
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
        } else if (arg == "--iterations" && i + 1 < argc) {
            config.read_iterations = std::stoull(argv[++i]);
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
    std::cout << CYAN << "║      Sequential Read Benchmark - Page Cache Performance      ║" << RESET << std::endl;
    std::cout << CYAN << "╚══════════════════════════════════════════════════════════════╝" << RESET << std::endl;

    SequentialReadBenchmark benchmark(config);
    benchmark.RunBenchmark();

    if (!csv_file.empty()) {
        benchmark.GetStats().SaveToCSV(csv_file);
        std::cout << "Results saved to: " << csv_file << std::endl;
    }

    return 0;
}
