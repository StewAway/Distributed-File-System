#include <iostream>
#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include "fs_master/user_context.hpp"

std::mutex result_mutex;
std::set<uint64_t> allocated_inode_ids;
std::set<uint64_t> allocated_block_ids;

void test_allocate_inodes(int num_inodes) {
    for (int i = 0; i < num_inodes; ++i) {
        uint64_t inode_id = fs_master::allocate_inode_id();
        {
            std::lock_guard<std::mutex> lock(result_mutex);
            if (allocated_inode_ids.count(inode_id) > 0) {
                std::cerr << "ERROR: Duplicate inode ID allocated: " << inode_id << std::endl;
            }
            allocated_inode_ids.insert(inode_id);
        }
    }
}

void test_allocate_blocks(int num_blocks) {
    for (int i = 0; i < num_blocks; ++i) {
        uint64_t block_id = fs_master::allocate_block_uuid();
        {
            std::lock_guard<std::mutex> lock(result_mutex);
            if (allocated_block_ids.count(block_id) > 0) {
                std::cerr << "ERROR: Duplicate block ID allocated: " << block_id << std::endl;
            }
            allocated_block_ids.insert(block_id);
        }
    }
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Testing Concurrent Allocation" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Test 1: Concurrent inode allocation
    std::cout << "Test 1: Concurrent Inode Allocation" << std::endl;
    std::cout << "  Spawning 10 threads, each allocating 100 inodes..." << std::endl;
    
    allocated_inode_ids.clear();
    std::vector<std::thread> inode_threads;
    for (int i = 0; i < 10; ++i) {
        inode_threads.emplace_back(test_allocate_inodes, 100);
    }
    
    for (auto& t : inode_threads) {
        t.join();
    }
    
    std::cout << "  ✓ Allocated " << allocated_inode_ids.size() << " unique inode IDs" << std::endl;
    if (allocated_inode_ids.size() == 1000) {
        std::cout << "  ✓ PASS: All 1000 inode IDs are unique" << std::endl;
    } else {
        std::cout << "  ✗ FAIL: Expected 1000 unique IDs, got " << allocated_inode_ids.size() << std::endl;
        return 1;
    }
    std::cout << std::endl;

    // Test 2: Concurrent block allocation
    std::cout << "Test 2: Concurrent Block Allocation" << std::endl;
    std::cout << "  Spawning 10 threads, each allocating 100 blocks..." << std::endl;
    
    allocated_block_ids.clear();
    std::vector<std::thread> block_threads;
    for (int i = 0; i < 10; ++i) {
        block_threads.emplace_back(test_allocate_blocks, 100);
    }
    
    for (auto& t : block_threads) {
        t.join();
    }
    
    std::cout << "  ✓ Allocated " << allocated_block_ids.size() << " unique block IDs" << std::endl;
    if (allocated_block_ids.size() == 1000) {
        std::cout << "  ✓ PASS: All 1000 block IDs are unique" << std::endl;
    } else {
        std::cout << "  ✗ FAIL: Expected 1000 unique IDs, got " << allocated_block_ids.size() << std::endl;
        return 1;
    }
    std::cout << std::endl;

    std::cout << "========================================" << std::endl;
    std::cout << "All tests passed!" << std::endl;
    std::cout << "========================================" << std::endl;

    return 0;
}
