/**
 * Benchmark: Random Read Performance Test
 * 
 * Purpose: Measure the read performance of the distributed file system
 * with random read patterns to stress test the page cache.
 * 
 * Test Scenario:
 * - Creates test files with known data
 * - Reads data at random file/offset combinations
 * - Measures cache hit effectiveness under random access
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
#include <set>
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
    uint64_t chunk_size_kb = 64;          // Read chunk size in KB (64KB = 1 block)
    uint64_t num_random_reads = 500;      // Number of random read operations
    uint64_t random_seed = 42;            // Random seed for reproducibility
    double hotspot_ratio = 0.2;           // Fraction of files/offsets that are "hot"
    double hotspot_access_prob = 0.8;     // Probability of accessing hot data
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
    uint64_t hot_accesses = 0;
    uint64_t cold_accesses = 0;
    
    // Per-phase stats
    std::vector<double> phase_throughputs;
    
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
    
    double GetMinLatencyMs() const {
        if (latencies_ms.empty()) return 0;
        return *std::min_element(latencies_ms.begin(), latencies_ms.end());
    }
    
    double GetMaxLatencyMs() const {
        if (latencies_ms.empty()) return 0;
        return *std::max_element(latencies_ms.begin(), latencies_ms.end());
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
        
        std::cout << "\n" << YELLOW << "Latency Statistics:" << RESET << std::endl;
        std::cout << "  Min Latency:     " << std::fixed << std::setprecision(2) 
                  << GetMinLatencyMs() << " ms" << std::endl;
        std::cout << "  Avg Latency:     " << std::fixed << std::setprecision(2) 
                  << GetAvgLatencyMs() << " ms" << std::endl;
        std::cout << "  P50 Latency:     " << std::fixed << std::setprecision(2) 
                  << GetP50LatencyMs() << " ms" << std::endl;
        std::cout << "  P99 Latency:     " << std::fixed << std::setprecision(2) 
                  << GetP99LatencyMs() << " ms" << std::endl;
        std::cout << "  Max Latency:     " << std::fixed << std::setprecision(2) 
                  << GetMaxLatencyMs() << " ms" << std::endl;
        
        std::cout << "\n" << YELLOW << "Random Access Pattern Analysis:" << RESET << std::endl;
        std::cout << "  Unique Files:    " << unique_files_accessed << std::endl;
        std::cout << "  Unique Offsets:  " << unique_offsets_accessed << std::endl;
        std::cout << "  Hot Accesses:    " << hot_accesses << " (" 
                  << std::fixed << std::setprecision(1) 
                  << (100.0 * hot_accesses / num_operations) << "%)" << std::endl;
        std::cout << "  Cold Accesses:   " << cold_accesses << " (" 
                  << std::fixed << std::setprecision(1) 
                  << (100.0 * cold_accesses / num_operations) << "%)" << std::endl;
        
        if (!phase_throughputs.empty()) {
            std::cout << "\n" << YELLOW << "Per-Phase Throughput (Cache Warming):" << RESET << std::endl;
            for (size_t i = 0; i < phase_throughputs.size(); ++i) {
                std::cout << "  Phase " << (i + 1) << ": " 
                          << std::fixed << std::setprecision(2) 
                          << phase_throughputs[i] << " MB/s";
                if (i > 0 && phase_throughputs[0] > 0) {
                    double speedup = phase_throughputs[i] / phase_throughputs[0];
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
                 << GetP99LatencyMs() << ","
                 << unique_files_accessed << ","
                 << unique_offsets_accessed << ","
                 << hot_accesses << ","
                 << cold_accesses << "\n";
            file.close();
        }
    }
};

class RandomReadBenchmark {
private:
    std::unique_ptr<FSMasterService::Stub> stub_;
    BenchmarkConfig config_;
    BenchmarkStats stats_;
    std::mt19937_64 rng_;
    
    // Track file descriptors
    std::vector<int> file_fds_;
    std::vector<std::string> file_paths_;
    
    // Hot spot data
    std::vector<uint64_t> hot_files_;
    std::vector<uint64_t> hot_offsets_;
    std::vector<uint64_t> cold_files_;
    std::vector<uint64_t> cold_offsets_;

public:
    RandomReadBenchmark(const BenchmarkConfig& config) 
        : config_(config), rng_(config.random_seed) {
        auto channel = grpc::CreateChannel(
            config_.master_addr,
            grpc::InsecureChannelCredentials()
        );
        stub_ = FSMasterService::NewStub(channel);
        stats_.test_name = "Random Read (Hotspot)";
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

    void SetupHotspots() {
        // Determine hot and cold files
        uint64_t num_hot_files = std::max(1UL, static_cast<uint64_t>(config_.num_files * config_.hotspot_ratio));
        
        for (uint64_t i = 0; i < config_.num_files; ++i) {
            if (i < num_hot_files) {
                hot_files_.push_back(i);
            } else {
                cold_files_.push_back(i);
            }
        }
        
        // Determine hot and cold offsets
        uint64_t num_offsets = config_.file_size_kb / config_.chunk_size_kb;
        uint64_t num_hot_offsets = std::max(1UL, static_cast<uint64_t>(num_offsets * config_.hotspot_ratio));
        
        for (uint64_t i = 0; i < num_offsets; ++i) {
            uint64_t offset = i * config_.chunk_size_kb * 1024;
            if (i < num_hot_offsets) {
                hot_offsets_.push_back(offset);
            } else {
                cold_offsets_.push_back(offset);
            }
        }
        
        std::cout << "Hotspot configuration:" << std::endl;
        std::cout << "  Hot files: " << hot_files_.size() << " / " << config_.num_files << std::endl;
        std::cout << "  Hot offsets: " << hot_offsets_.size() << " / " << num_offsets << std::endl;
        std::cout << "  Access probability to hot data: " << (config_.hotspot_access_prob * 100) << "%" << std::endl;
    }

    bool SetupTestFiles() {
        std::cout << "\nSetting up test files..." << std::endl;
        
        std::string chunk_data = GenerateData(config_.chunk_size_kb * 1024);
        uint64_t chunks_per_file = config_.file_size_kb / config_.chunk_size_kb;

        for (uint64_t file_idx = 0; file_idx < config_.num_files; ++file_idx) {
            std::string filepath = "/benchmark/rand_read_" + std::to_string(file_idx) + ".dat";
            file_paths_.push_back(filepath);
            
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
        
        // Now open all files for random access
        std::cout << "Opening files for random access..." << std::endl;
        for (const auto& path : file_paths_) {
            int fd = OpenFile(path, "r");
            if (fd < 0) {
                std::cerr << RED << "Failed to open file for random access: " << path << RESET << std::endl;
                return false;
            }
            file_fds_.push_back(fd);
        }
        
        return true;
    }

    std::pair<uint64_t, uint64_t> GetRandomAccess() {
        std::uniform_real_distribution<double> prob_dist(0.0, 1.0);
        
        uint64_t file_idx, offset;
        bool is_hot = prob_dist(rng_) < config_.hotspot_access_prob;
        
        if (is_hot && !hot_files_.empty() && !hot_offsets_.empty()) {
            std::uniform_int_distribution<size_t> hot_file_dist(0, hot_files_.size() - 1);
            std::uniform_int_distribution<size_t> hot_offset_dist(0, hot_offsets_.size() - 1);
            file_idx = hot_files_[hot_file_dist(rng_)];
            offset = hot_offsets_[hot_offset_dist(rng_)];
            stats_.hot_accesses++;
        } else {
            // Access cold data or fall back if no hot data
            std::vector<uint64_t>& file_pool = cold_files_.empty() ? hot_files_ : cold_files_;
            std::vector<uint64_t>& offset_pool = cold_offsets_.empty() ? hot_offsets_ : cold_offsets_;
            
            std::uniform_int_distribution<size_t> file_dist(0, file_pool.size() - 1);
            std::uniform_int_distribution<size_t> offset_dist(0, offset_pool.size() - 1);
            file_idx = file_pool[file_dist(rng_)];
            offset = offset_pool[offset_dist(rng_)];
            stats_.cold_accesses++;
        }
        
        return {file_idx, offset};
    }

    void RunBenchmark() {
        std::cout << "\n" << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "Random Read Benchmark (Hotspot)" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << "Files: " << config_.num_files << std::endl;
        std::cout << "File Size: " << config_.file_size_kb << " KB" << std::endl;
        std::cout << "Chunk Size: " << config_.chunk_size_kb << " KB" << std::endl;
        std::cout << "Random Reads: " << config_.num_random_reads << std::endl;
        std::cout << "Random Seed: " << config_.random_seed << std::endl;

        if (!Mount()) {
            std::cerr << RED << "Failed to mount, aborting benchmark" << RESET << std::endl;
            return;
        }

        // Setup hotspot configuration
        SetupHotspots();

        // Setup test files first
        if (!SetupTestFiles()) {
            std::cerr << RED << "Failed to setup test files, aborting" << RESET << std::endl;
            UnMount();
            return;
        }

        std::cout << "\nStarting random read benchmark..." << std::endl;
        
        uint64_t read_size = config_.chunk_size_kb * 1024;
        
        // Track unique accesses
        std::set<uint64_t> unique_files;
        std::set<std::pair<uint64_t, uint64_t>> unique_file_offsets;
        
        // Run in phases to observe cache warming
        const uint64_t num_phases = 5;
        uint64_t ops_per_phase = config_.num_random_reads / num_phases;

        auto benchmark_start = std::chrono::high_resolution_clock::now();

        for (uint64_t phase = 0; phase < num_phases; ++phase) {
            std::cout << "\n" << YELLOW << "Phase " << (phase + 1) << "/" 
                      << num_phases << RESET << std::endl;
            
            auto phase_start = std::chrono::high_resolution_clock::now();
            uint64_t phase_bytes = 0;

            for (uint64_t op = 0; op < ops_per_phase; ++op) {
                auto [file_idx, offset] = GetRandomAccess();
                
                unique_files.insert(file_idx);
                unique_file_offsets.insert({file_idx, offset});
                
                int fd = file_fds_[file_idx];
                
                if (config_.verbose) {
                    std::cout << "Reading from file " << file_idx << " at offset " << offset << std::endl;
                }

                auto op_start = std::chrono::high_resolution_clock::now();
                auto [success, bytes_read] = ReadFile(fd, read_size);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                stats_.num_operations++;
                if (success && bytes_read > 0) {
                    stats_.successful_ops++;
                    stats_.total_bytes += bytes_read;
                    phase_bytes += bytes_read;
                    
                    double latency_ms = std::chrono::duration<double, std::milli>(
                        op_end - op_start).count();
                    stats_.latencies_ms.push_back(latency_ms);
                } else {
                    stats_.failed_ops++;
                }
            }

            auto phase_end = std::chrono::high_resolution_clock::now();
            double phase_time = std::chrono::duration<double>(phase_end - phase_start).count();
            double phase_throughput = (phase_bytes / (1024.0 * 1024.0)) / phase_time;
            stats_.phase_throughputs.push_back(phase_throughput);
            
            std::cout << "  Throughput: " << std::fixed << std::setprecision(2) 
                      << phase_throughput << " MB/s" << std::endl;
        }

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
    std::cout << "  --chunk-size <kb>   Read chunk size in KB (default: 64)" << std::endl;
    std::cout << "  --reads <n>         Number of random reads (default: 500)" << std::endl;
    std::cout << "  --seed <n>          Random seed (default: 42)" << std::endl;
    std::cout << "  --hotspot-ratio <r> Fraction of data that is 'hot' (default: 0.2)" << std::endl;
    std::cout << "  --hotspot-prob <p>  Probability of accessing hot data (default: 0.8)" << std::endl;
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
        } else if (arg == "--reads" && i + 1 < argc) {
            config.num_random_reads = std::stoull(argv[++i]);
        } else if (arg == "--seed" && i + 1 < argc) {
            config.random_seed = std::stoull(argv[++i]);
        } else if (arg == "--hotspot-ratio" && i + 1 < argc) {
            config.hotspot_ratio = std::stod(argv[++i]);
        } else if (arg == "--hotspot-prob" && i + 1 < argc) {
            config.hotspot_access_prob = std::stod(argv[++i]);
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
    std::cout << CYAN << "║       Random Read Benchmark - Page Cache Performance         ║" << RESET << std::endl;
    std::cout << CYAN << "╚══════════════════════════════════════════════════════════════╝" << RESET << std::endl;

    RandomReadBenchmark benchmark(config);
    benchmark.RunBenchmark();

    if (!csv_file.empty()) {
        benchmark.GetStats().SaveToCSV(csv_file);
        std::cout << "Results saved to: " << csv_file << std::endl;
    }

    return 0;
}
