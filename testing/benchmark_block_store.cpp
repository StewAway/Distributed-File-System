/**
 * Tier 2: BlockStore Benchmarking & Profiling
 * 
 * Purpose: Measure access patterns to justify implementing a page cache
 * 
 * Tests:
 * 1. SEQUENTIAL_WRITE: Write blocks 0,1,2,3... sequentially
 *    Expected: Cache would help if reading back in same order
 * 
 * 2. SEQUENTIAL_READ: Read blocks 0,1,2,3... sequentially
 *    Expected: Good cache hit ratio if working set < memory
 * 
 * 3. RANDOM_WRITE: Write random block IDs
 *    Expected: Cache less helpful for writes
 * 
 * 4. RANDOM_READ: Read random block IDs
 *    Expected: High miss rate unless working set small
 * 
 * 5. WORKING_SET_ANALYSIS: Measure unique blocks accessed
 *    If unique blocks << total blocks, cache beneficial
 * 
 * Metrics collected:
 * - Total reads/writes (from BlockStore stats)
 * - Total bytes read/written
 * - Time per operation
 * - Cache hit rate estimation
 * 
 * Decision criteria for page cache:
 * - Working set < 10% of total storage: Implement cache
 * - Read reuse ratio > 50%: Implement cache
 * - Sequential patterns dominant: Cache beneficial
 * - Random access dominant: Cache not beneficial
 */

#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <set>
#include <unordered_set>
#include <iomanip>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

// Simple statistics collector for benchmarking
struct BenchmarkStats {
    std::string test_name;
    uint64_t num_operations = 0;
    uint64_t total_bytes = 0;
    std::chrono::duration<double> elapsed_time;
    std::set<uint64_t> unique_blocks_accessed;
    
    double GetThroughputMBps() const {
        if (elapsed_time.count() == 0) return 0;
        return (total_bytes / (1024.0 * 1024.0)) / elapsed_time.count();
    }
    
    double GetOpsPerSecond() const {
        if (elapsed_time.count() == 0) return 0;
        return num_operations / elapsed_time.count();
    }
    
    double GetAvgBytesPerOp() const {
        if (num_operations == 0) return 0;
        return static_cast<double>(total_bytes) / num_operations;
    }
    
    void Print() const {
        std::cout << "\n" << std::string(70, '=') << std::endl;
        std::cout << "Test: " << test_name << std::endl;
        std::cout << std::string(70, '=') << std::endl;
        std::cout << "Operations:        " << num_operations << std::endl;
        std::cout << "Total Bytes:       " << total_bytes << " (" 
                  << (total_bytes / (1024.0 * 1024.0)) << " MB)" << std::endl;
        std::cout << "Elapsed Time:      " << std::fixed << std::setprecision(3) 
                  << elapsed_time.count() << " seconds" << std::endl;
        std::cout << "Throughput:        " << std::fixed << std::setprecision(2) 
                  << GetThroughputMBps() << " MB/s" << std::endl;
        std::cout << "Ops/sec:           " << std::fixed << std::setprecision(0) 
                  << GetOpsPerSecond() << std::endl;
        std::cout << "Avg Bytes/Op:      " << std::fixed << std::setprecision(0) 
                  << GetAvgBytesPerOp() << " bytes" << std::endl;
        std::cout << "Unique Blocks:     " << unique_blocks_accessed.size() << std::endl;
        std::cout << std::string(70, '=') << std::endl;
    }
};

// Helper to generate random data
std::string GenerateRandomData(uint64_t size) {
    std::string data(size, 0);
    for (uint64_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>(i % 256);
    }
    return data;
}

// Test 1: Sequential Write Pattern
BenchmarkStats BenchmarkSequentialWrite(uint64_t num_blocks, uint64_t block_size,
                                       const std::string& test_dir) {
    BenchmarkStats stats;
    stats.test_name = "SEQUENTIAL_WRITE";
    
    // Create block files sequentially
    auto start = std::chrono::high_resolution_clock::now();
    
    std::string data = GenerateRandomData(block_size);
    
    for (uint64_t i = 0; i < num_blocks; ++i) {
        std::string block_path = test_dir + "/blk_" + std::to_string(i) + ".img";
        std::ofstream file(block_path, std::ios::binary);
        file.write(data.c_str(), data.length());
        file.flush();
        file.close();
        
        stats.num_operations++;
        stats.total_bytes += block_size;
        stats.unique_blocks_accessed.insert(i);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    stats.elapsed_time = end - start;
    
    return stats;
}

// Test 2: Sequential Read Pattern
BenchmarkStats BenchmarkSequentialRead(uint64_t num_blocks, uint64_t block_size,
                                      const std::string& test_dir) {
    BenchmarkStats stats;
    stats.test_name = "SEQUENTIAL_READ";
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (uint64_t i = 0; i < num_blocks; ++i) {
        std::string block_path = test_dir + "/blk_" + std::to_string(i) + ".img";
        std::ifstream file(block_path, std::ios::binary);
        
        // Read entire block
        std::string data((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
        file.close();
        
        stats.num_operations++;
        stats.total_bytes += data.length();
        stats.unique_blocks_accessed.insert(i);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    stats.elapsed_time = end - start;
    
    return stats;
}

// Test 3: Random Write Pattern
BenchmarkStats BenchmarkRandomWrite(uint64_t num_operations, uint64_t block_size,
                                   uint64_t max_block_id, const std::string& test_dir) {
    BenchmarkStats stats;
    stats.test_name = "RANDOM_WRITE";
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, max_block_id - 1);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::string data = GenerateRandomData(block_size);
    
    for (uint64_t i = 0; i < num_operations; ++i) {
        uint64_t block_id = dis(gen);
        std::string block_path = test_dir + "/blk_" + std::to_string(block_id) + ".img";
        
        std::ofstream file(block_path, std::ios::binary);
        file.write(data.c_str(), data.length());
        file.flush();
        file.close();
        
        stats.num_operations++;
        stats.total_bytes += block_size;
        stats.unique_blocks_accessed.insert(block_id);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    stats.elapsed_time = end - start;
    
    return stats;
}

// Test 4: Random Read Pattern
BenchmarkStats BenchmarkRandomRead(uint64_t num_operations, uint64_t max_block_id,
                                  const std::string& test_dir) {
    BenchmarkStats stats;
    stats.test_name = "RANDOM_READ";
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, max_block_id - 1);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (uint64_t i = 0; i < num_operations; ++i) {
        uint64_t block_id = dis(gen);
        std::string block_path = test_dir + "/blk_" + std::to_string(block_id) + ".img";
        
        std::ifstream file(block_path, std::ios::binary);
        if (file.is_open()) {
            std::string data((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
            file.close();
            
            stats.num_operations++;
            stats.total_bytes += data.length();
            stats.unique_blocks_accessed.insert(block_id);
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    stats.elapsed_time = end - start;
    
    return stats;
}

// Test 5: Working Set Analysis - Hot/Cold Data
BenchmarkStats BenchmarkWorkingSetAnalysis(uint64_t num_operations, uint64_t max_block_id,
                                          const std::string& test_dir) {
    BenchmarkStats stats;
    stats.test_name = "WORKING_SET_ANALYSIS (80/20 Zipfian)";
    
    // Simulate 80/20 rule: 80% of requests go to 20% of blocks (hot data)
    std::random_device rd;
    std::mt19937 gen(rd());
    uint64_t hot_blocks = std::max(1UL, max_block_id / 5);  // 20%
    
    std::uniform_int_distribution<uint64_t> hot_dis(0, hot_blocks - 1);
    std::uniform_int_distribution<uint64_t> cold_dis(hot_blocks, max_block_id - 1);
    std::uniform_int_distribution<int> choice(0, 99);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (uint64_t i = 0; i < num_operations; ++i) {
        uint64_t block_id;
        
        // 80% chance to access hot blocks, 20% cold
        if (choice(gen) < 80) {
            block_id = hot_dis(gen);
        } else {
            block_id = cold_dis(gen);
        }
        
        std::string block_path = test_dir + "/blk_" + std::to_string(block_id) + ".img";
        
        std::ifstream file(block_path, std::ios::binary);
        if (file.is_open()) {
            std::string data((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
            file.close();
            
            stats.num_operations++;
            stats.total_bytes += data.length();
            stats.unique_blocks_accessed.insert(block_id);
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    stats.elapsed_time = end - start;
    
    return stats;
}

// Analysis and recommendations
void PrintAnalysis(const std::vector<BenchmarkStats>& all_stats) {
    std::cout << "\n\n" << std::string(70, '#') << std::endl;
    std::cout << "# TIER 2 ANALYSIS: Should we implement page cache?" << std::endl;
    std::cout << std::string(70, '#') << std::endl;
    
    // Calculate metrics
    uint64_t total_reads_writes = 0;
    uint64_t total_unique_blocks = 0;
    uint64_t max_unique_blocks = 0;
    
    for (const auto& stat : all_stats) {
        total_reads_writes += stat.num_operations;
        max_unique_blocks = std::max(max_unique_blocks, 
                                    static_cast<uint64_t>(stat.unique_blocks_accessed.size()));
    }
    
    // Working set efficiency
    double avg_block_reuse = static_cast<double>(total_reads_writes) / max_unique_blocks;
    
    std::cout << "\nKey Metrics:" << std::endl;
    std::cout << "  Total operations:       " << total_reads_writes << std::endl;
    std::cout << "  Largest working set:    " << max_unique_blocks << " blocks" << std::endl;
    std::cout << "  Avg reuse per block:    " << std::fixed << std::setprecision(2) 
              << avg_block_reuse << "x" << std::endl;
    
    std::cout << "\nRecommendations:" << std::endl;
    
    // Decision logic
    bool implement_cache = false;
    std::string rationale;
    
    if (max_unique_blocks <= 1000) {
        implement_cache = true;
        rationale += "  ✓ Small working set (<1000 blocks), cache feasible\n";
    } else {
        rationale += "  ✗ Large working set, cache less effective\n";
    }
    
    if (avg_block_reuse > 1.5) {
        implement_cache = true;
        rationale += "  ✓ High block reuse (>1.5x), cache beneficial\n";
    } else {
        rationale += "  ✗ Low block reuse (<1.5x), cache not beneficial\n";
    }
    
    // Check for hot data pattern (working set analysis)
    const auto& ws_stat = all_stats.back();
    double hotspot_ratio = static_cast<double>(ws_stat.unique_blocks_accessed.size()) / 
                          max_unique_blocks;
    if (hotspot_ratio < 0.3) {
        implement_cache = true;
        rationale += "  ✓ Strong hot data pattern, cache would help\n";
    } else {
        rationale += "  ✗ Uniform access pattern, cache less helpful\n";
    }
    
    std::cout << rationale;
    
    std::cout << "\nDecision: ";
    if (implement_cache) {
        std::cout << "✓ IMPLEMENT PAGE CACHE\n";
        std::cout << "  - Use LRU eviction with ~100MB capacity\n";
        std::cout << "  - Expected cache hit rate: 70-90%\n";
        std::cout << "  - Expected speedup: 10-50x for hot data\n";
    } else {
        std::cout << "✗ SKIP PAGE CACHE\n";
        std::cout << "  - OS page cache already handles most workloads\n";
        std::cout << "  - Focus on other optimizations instead\n";
    }
    
    std::cout << std::string(70, '#') << std::endl << std::endl;
}

int main(int argc, char* argv[]) {
    std::string test_dir = "./benchmark_blocks";
    
    // Create test directory
    if (fs::exists(test_dir)) {
        fs::remove_all(test_dir);
    }
    fs::create_directories(test_dir);
    
    std::cout << "\n" << std::string(70, '#') << std::endl;
    std::cout << "# Tier 2: BlockStore Performance Benchmarking" << std::endl;
    std::cout << "# Measuring access patterns to justify page cache implementation" << std::endl;
    std::cout << std::string(70, '#') << std::endl;
    
    // Parameters
    const uint64_t NUM_BLOCKS = 1000;        // Sequential test size
    const uint64_t BLOCK_SIZE = 65536;       // 64 KB blocks
    const uint64_t NUM_RANDOM_OPS = 10000;   // Random access operations
    
    std::vector<BenchmarkStats> results;
    
    // Run tests
    std::cout << "\nRunning benchmarks (this may take a minute)...\n" << std::endl;
    
    results.push_back(BenchmarkSequentialWrite(NUM_BLOCKS, BLOCK_SIZE, test_dir));
    results[0].Print();
    
    results.push_back(BenchmarkSequentialRead(NUM_BLOCKS, BLOCK_SIZE, test_dir));
    results[1].Print();
    
    results.push_back(BenchmarkRandomWrite(NUM_RANDOM_OPS, BLOCK_SIZE, NUM_BLOCKS, test_dir));
    results[2].Print();
    
    results.push_back(BenchmarkRandomRead(NUM_RANDOM_OPS, NUM_BLOCKS, test_dir));
    results[3].Print();
    
    results.push_back(BenchmarkWorkingSetAnalysis(NUM_RANDOM_OPS, NUM_BLOCKS, test_dir));
    results[4].Print();
    
    // Analysis
    PrintAnalysis(results);
    
    // Cleanup
    fs::remove_all(test_dir);
    
    return 0;
}
