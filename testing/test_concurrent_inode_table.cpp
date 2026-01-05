#include <iostream>
#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include "fs_master/user_context.hpp"

std::mutex result_mutex;
std::set<uint64_t> read_inode_ids;
std::set<uint64_t> written_inode_ids;
int read_count = 0;
int write_count = 0;

void write_inodes(int num_inodes) {
    for (int i = 0; i < num_inodes; ++i) {
        uint64_t inode_id = fs_master::allocate_inode_id();
        fs_master::Inode inode(inode_id, false);  // is_directory = false
        inode.size = i * 100;
        fs_master::PutInode(inode_id, inode);
        
        {
            std::lock_guard<std::mutex> lock(result_mutex);
            written_inode_ids.insert(inode_id);
            write_count++;
        }
    }
}

void read_inodes(int num_reads) {
    for (int i = 0; i < num_reads; ++i) {
        // Read a random inode that might exist
        uint64_t inode_id = i % 1000;  // Try to read from first 1000 possible IDs
        
        auto inode = fs_master::GetInode(inode_id);
        if (inode.has_value()) {
            {
                std::lock_guard<std::mutex> lock(result_mutex);
                read_inode_ids.insert(inode_id);
                read_count++;
            }
        }
    }
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Testing Concurrent Inode Table Access" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Test 1: Concurrent writes
    std::cout << "Test 1: Concurrent Inode Writes" << std::endl;
    std::cout << "  Spawning 5 threads, each writing 100 inodes..." << std::endl;
    
    written_inode_ids.clear();
    std::vector<std::thread> write_threads;
    for (int i = 0; i < 5; ++i) {
        write_threads.emplace_back(write_inodes, 100);
    }
    
    for (auto& t : write_threads) {
        t.join();
    }
    
    std::cout << "  ✓ Written " << write_count << " inodes" << std::endl;
    std::cout << "  ✓ Unique inode IDs created: " << written_inode_ids.size() << std::endl;
    if (written_inode_ids.size() == 500) {
        std::cout << "  ✓ PASS: All 500 inode IDs are unique" << std::endl;
    } else {
        std::cout << "  ✗ FAIL: Expected 500 unique IDs, got " << written_inode_ids.size() << std::endl;
        return 1;
    }
    std::cout << std::endl;

    // Test 2: Concurrent reads while data exists
    std::cout << "Test 2: Concurrent Inode Reads" << std::endl;
    std::cout << "  Spawning 10 threads, each reading 500 times..." << std::endl;
    
    read_count = 0;
    read_inode_ids.clear();
    std::vector<std::thread> read_threads;
    for (int i = 0; i < 10; ++i) {
        read_threads.emplace_back(read_inodes, 500);
    }
    
    for (auto& t : read_threads) {
        t.join();
    }
    
    std::cout << "  ✓ Performed " << read_count << " successful reads" << std::endl;
    std::cout << "  ✓ Unique inodes read: " << read_inode_ids.size() << std::endl;
    if (read_count > 0) {
        std::cout << "  ✓ PASS: Concurrent reads working correctly" << std::endl;
    } else {
        std::cout << "  ✗ FAIL: No successful reads performed" << std::endl;
        return 1;
    }
    std::cout << std::endl;

    // Test 3: InodeExists and DeleteInode
    std::cout << "Test 3: InodeExists and DeleteInode" << std::endl;
    uint64_t test_inode_id = *written_inode_ids.begin();
    
    if (fs_master::InodeExists(test_inode_id)) {
        std::cout << "  ✓ InodeExists returns true for created inode " << test_inode_id << std::endl;
    } else {
        std::cout << "  ✗ FAIL: InodeExists should return true" << std::endl;
        return 1;
    }
    
    size_t table_size_before = fs_master::GetInodeTableSize();
    bool deleted = fs_master::DeleteInode(test_inode_id);
    size_t table_size_after = fs_master::GetInodeTableSize();
    
    if (deleted && table_size_after == table_size_before - 1) {
        std::cout << "  ✓ DeleteInode successful" << std::endl;
        std::cout << "  ✓ Table size: " << table_size_before << " -> " << table_size_after << std::endl;
    } else {
        std::cout << "  ✗ FAIL: DeleteInode didn't work correctly" << std::endl;
        return 1;
    }
    
    if (!fs_master::InodeExists(test_inode_id)) {
        std::cout << "  ✓ PASS: InodeExists returns false after deletion" << std::endl;
    } else {
        std::cout << "  ✗ FAIL: InodeExists should return false after deletion" << std::endl;
        return 1;
    }
    std::cout << std::endl;

    std::cout << "========================================" << std::endl;
    std::cout << "All tests passed!" << std::endl;
    std::cout << "========================================" << std::endl;

    return 0;
}
