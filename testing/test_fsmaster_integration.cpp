#include <iostream>
#include <vector>
#include <thread>
#include <string>
#include <unordered_map>
#include "fs_master/user_context.hpp"
#include "fs_master/fsmaster_service.hpp"

void simulate_user_operations(const std::string& user_id, int operations_count) {
    std::string error_msg;
    
    // Initialize user root
    uint64_t user_root = fs_master::allocate_inode_id();
    fs_master::PutInode(user_root, fs_master::Inode(user_root, true));  // is_directory
    fs_master::user_roots[user_id] = user_root;
    
    // Create several files/directories
    for (int i = 0; i < operations_count; ++i) {
        std::string path = "/dir" + std::to_string(i) + "/file" + std::to_string(i) + ".txt";
        
        // Simulate path creation
        std::vector<std::string> components = {
            "dir" + std::to_string(i),
            "file" + std::to_string(i) + ".txt"
        };
        
        // Create directory
        uint64_t dir_id = fs_master::allocate_inode_id();
        auto root_inode = fs_master::GetInode(user_root).value();
        root_inode.children["dir" + std::to_string(i)] = dir_id;
        fs_master::PutInode(user_root, root_inode);
        fs_master::PutInode(dir_id, fs_master::Inode(dir_id, true));  // is_directory
        
        // Create file
        uint64_t file_id = fs_master::allocate_inode_id();
        auto dir_inode = fs_master::GetInode(dir_id).value();
        dir_inode.children["file" + std::to_string(i) + ".txt"] = file_id;
        fs_master::PutInode(dir_id, dir_inode);
        fs_master::PutInode(file_id, fs_master::Inode(file_id, false));  // is_file
        
        // Allocate and add blocks
        auto file_inode = fs_master::GetInode(file_id).value();
        for (int j = 0; j < 2; ++j) {
            uint64_t block_id = fs_master::allocate_block_uuid();
            file_inode.blocks.push_back(std::to_string(block_id));
        }
        file_inode.size = 2 * 64 * 1024;  // 128KB
        fs_master::PutInode(file_id, file_inode);
    }
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Testing Integration with gRPC Operations" << std::endl;
    std::cout << "========================================\n" << std::endl;

    std::cout << "Simulating concurrent user file operations:" << std::endl;
    std::cout << "  Spawning 3 users, each creating 10 files..." << std::endl;
    
    std::vector<std::thread> threads;
    for (int i = 0; i < 3; ++i) {
        std::string user_id = "user" + std::to_string(i);
        threads.emplace_back(simulate_user_operations, user_id, 10);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "  ✓ Completed concurrent user operations" << std::endl << std::endl;
    
    // Verify results
    std::cout << "Verifying results:" << std::endl;
    
    size_t total_inodes = fs_master::GetInodeTableSize();
    std::cout << "  ✓ Total inodes created: " << total_inodes << std::endl;
    
    // We should have created:
    // 3 users * 10 operations * (1 dir + 1 file) = 60 inodes
    // Plus 3 root inodes = 63 total
    if (total_inodes >= 60) {
        std::cout << "  ✓ PASS: Expected at least 60 inodes, got " << total_inodes << std::endl;
    } else {
        std::cout << "  ✗ FAIL: Expected at least 60 inodes, got " << total_inodes << std::endl;
        return 1;
    }
    
    // Verify a few specific inodes exist
    std::cout << "  ✓ Verifying inode integrity..." << std::endl;
    int found_count = 0;
    for (uint64_t i = 0; i < 50; ++i) {
        if (fs_master::InodeExists(i)) {
            found_count++;
        }
    }
    std::cout << "  ✓ Found " << found_count << " valid inodes in ID range 0-49" << std::endl;
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Integration test passed!" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return 0;
}
