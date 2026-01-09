#include <iostream>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>
#include "fs_server/block_store.hpp"

namespace fs = std::filesystem;

// Helper to read file contents directly from disk (bypassing cache)
std::string read_disk_contents(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return "";
    return std::string((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
}

// Helper to get block file path
std::string get_block_path(const std::string& dir, uint64_t block_uuid) {
    return dir + "/blk_" + std::to_string(block_uuid) + ".img";
}

// ============================================================================
// Test 1: Basic write and read entire block
// ============================================================================
void test_basic_write_read() {
    std::cout << "\n=== Test 1: Basic write and read ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_basic";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write a block (offset=0 means write entire block)
        std::string write_data = "Hello, BlockStore!";
        bool success = store.WriteBlock(1, 0, write_data, true);
        assert(success && "WriteBlock should succeed");
        std::cout << "  Wrote block 1: '" << write_data << "'" << std::endl;
        
        // Read entire block back
        std::string read_data;
        success = store.ReadBlock(1, 0, 0, read_data);
        assert(success && "ReadBlock should succeed");
        assert(read_data == write_data && "Read data should match written data");
        std::cout << "  Read block 1: '" << read_data << "'" << std::endl;
        
        // Verify on disk
        std::string disk_data = read_disk_contents(get_block_path(test_dir, 1));
        assert(disk_data == write_data && "Disk data should match");
        std::cout << "  Verified on disk: '" << disk_data << "'" << std::endl;
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 2: Partial write at offset (Read-Modify-Write)
// ============================================================================
void test_partial_write() {
    std::cout << "\n=== Test 2: Partial write at offset ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_partial_write";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write initial block
        std::string initial = "AAAAAAAAAA";  // 10 A's
        store.WriteBlock(1, 0, initial, true);
        std::cout << "  Initial block: '" << initial << "'" << std::endl;
        
        // Partial write at offset 3
        store.WriteBlock(1, 3, "BBB", true);
        std::cout << "  Wrote 'BBB' at offset 3" << std::endl;
        
        // Read back
        std::string result;
        store.ReadBlock(1, 0, 0, result);
        std::cout << "  Result: '" << result << "'" << std::endl;
        assert(result == "AAABBBAAAA" && "Partial write should modify middle");
        
        // Another partial write at offset 7
        store.WriteBlock(1, 7, "CCC", true);
        std::cout << "  Wrote 'CCC' at offset 7" << std::endl;
        
        store.ReadBlock(1, 0, 0, result);
        std::cout << "  Result: '" << result << "'" << std::endl;
        assert(result == "AAABBBACC" + std::string(1, 'C') && "Second partial write");
        // Actually: "AAABBBACCC" (extends the block)
        
        // Partial write that extends the block
        store.WriteBlock(1, 12, "DDD", true);
        std::cout << "  Wrote 'DDD' at offset 12 (extends block)" << std::endl;
        
        store.ReadBlock(1, 0, 0, result);
        std::cout << "  Result: '" << result << "' (length=" << result.length() << ")" << std::endl;
        assert(result.length() == 15 && "Block should be extended to 15 bytes");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 3: Partial read with offset and length
// ============================================================================
void test_partial_read() {
    std::cout << "\n=== Test 3: Partial read with offset and length ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_partial_read";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write a block with known content
        std::string data = "0123456789ABCDEFGHIJ";  // 20 bytes
        store.WriteBlock(1, 0, data, true);
        std::cout << "  Wrote block: '" << data << "'" << std::endl;
        
        std::string result;
        
        // Read entire block (offset=0, length=0)
        store.ReadBlock(1, 0, 0, result);
        assert(result == data && "Full read should return entire block");
        std::cout << "  Read(0, 0): '" << result << "'" << std::endl;
        
        // Read first 5 bytes
        store.ReadBlock(1, 0, 5, result);
        assert(result == "01234" && "First 5 bytes");
        std::cout << "  Read(0, 5): '" << result << "'" << std::endl;
        
        // Read 5 bytes from offset 5
        store.ReadBlock(1, 5, 5, result);
        assert(result == "56789" && "5 bytes from offset 5");
        std::cout << "  Read(5, 5): '" << result << "'" << std::endl;
        
        // Read from offset 10 to end
        store.ReadBlock(1, 10, 0, result);
        assert(result == "ABCDEFGHIJ" && "From offset 10 to end");
        std::cout << "  Read(10, 0): '" << result << "'" << std::endl;
        
        // Read last 3 bytes
        store.ReadBlock(1, 17, 3, result);
        assert(result == "HIJ" && "Last 3 bytes");
        std::cout << "  Read(17, 3): '" << result << "'" << std::endl;
        
        // Read beyond block (should clamp to available data)
        store.ReadBlock(1, 18, 10, result);
        assert(result == "IJ" && "Should clamp to available data");
        std::cout << "  Read(18, 10): '" << result << "' (clamped)" << std::endl;
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 4: Cache hit vs cache miss
// ============================================================================
void test_cache_behavior() {
    std::cout << "\n=== Test 4: Cache hit vs cache miss ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_cache";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write block 1
        store.WriteBlock(1, 0, "block1_data", true);
        std::cout << "  Wrote block 1" << std::endl;
        
        // Read block 1 - should be cache hit (was just written)
        std::string result;
        std::cout << "  Reading block 1 (should be cache hit)..." << std::endl;
        store.ReadBlock(1, 0, 0, result);
        assert(result == "block1_data");
        
        // Read block 1 again - should still be cache hit
        std::cout << "  Reading block 1 again (should be cache hit)..." << std::endl;
        store.ReadBlock(1, 0, 0, result);
        assert(result == "block1_data");
    }
    
    // Restart store - cache is empty, should read from disk
    {
        fs_server::BlockStore store(test_dir, true);
        
        std::string result;
        std::cout << "  After restart, reading block 1 (should be cache miss -> disk read)..." << std::endl;
        store.ReadBlock(1, 0, 0, result);
        assert(result == "block1_data" && "Should read from disk after restart");
        
        // Second read should be cache hit
        std::cout << "  Reading block 1 again (should now be cache hit)..." << std::endl;
        store.ReadBlock(1, 0, 0, result);
        assert(result == "block1_data");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 5: Write-back cache (dirty page writeback on destruction)
// ============================================================================
void test_writeback_on_destruction() {
    std::cout << "\n=== Test 5: Write-back cache behavior ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_writeback";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write new block (goes to disk immediately since it's new)
        store.WriteBlock(1, 0, "version1", true);
        std::cout << "  Wrote new block 1: 'version1'" << std::endl;
        
        // Update block without sync (dirty in cache only)
        store.WriteBlock(1, 0, "version2", false);  // sync=false
        std::cout << "  Updated block 1 to 'version2' (sync=false, dirty in cache)" << std::endl;
        
        // Check disk - should still have version1
        std::string disk_data = read_disk_contents(get_block_path(test_dir, 1));
        std::cout << "  Disk has: '" << disk_data << "'" << std::endl;
        assert(disk_data == "version1" && "Disk should still have version1 (write-back)");
        
        // Read from cache should return version2
        std::string cache_data;
        store.ReadBlock(1, 0, 0, cache_data);
        std::cout << "  Cache has: '" << cache_data << "'" << std::endl;
        assert(cache_data == "version2" && "Cache should have version2");
        
        std::cout << "  Destroying BlockStore (should flush dirty pages)..." << std::endl;
    }
    // Destructor should have flushed version2 to disk
    
    // Verify disk now has version2
    std::string disk_data = read_disk_contents(get_block_path(test_dir, 1));
    std::cout << "  After destruction, disk has: '" << disk_data << "'" << std::endl;
    assert(disk_data == "version2" && "Disk should have version2 after flush");
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 6: Sync write goes to disk immediately
// ============================================================================
void test_sync_write() {
    std::cout << "\n=== Test 6: Sync write behavior ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_sync";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write new block with sync
        store.WriteBlock(1, 0, "initial", true);  // sync=true
        
        // Update with sync=true (should go to disk immediately)
        store.WriteBlock(1, 0, "updated", true);  // sync=true
        std::cout << "  Wrote block 1 with sync=true: 'updated'" << std::endl;
        
        // Check disk immediately
        std::string disk_data = read_disk_contents(get_block_path(test_dir, 1));
        std::cout << "  Disk has (immediately): '" << disk_data << "'" << std::endl;
        assert(disk_data == "updated" && "Sync write should go to disk immediately");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 7: Multiple blocks
// ============================================================================
void test_multiple_blocks() {
    std::cout << "\n=== Test 7: Multiple blocks ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_multi";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write multiple blocks
        for (uint64_t i = 1; i <= 5; i++) {
            std::string data = "block_" + std::to_string(i) + "_data";
            store.WriteBlock(i, 0, data, true);
        }
        std::cout << "  Wrote blocks 1-5" << std::endl;
        
        // Read them back in reverse order
        for (uint64_t i = 5; i >= 1; i--) {
            std::string expected = "block_" + std::to_string(i) + "_data";
            std::string result;
            store.ReadBlock(i, 0, 0, result);
            assert(result == expected && "Block data should match");
        }
        std::cout << "  Read blocks 5-1, all match" << std::endl;
        
        // Update some blocks
        store.WriteBlock(2, 0, "block_2_updated", true);
        store.WriteBlock(4, 0, "block_4_updated", true);
        std::cout << "  Updated blocks 2 and 4" << std::endl;
        
        // Verify updates
        std::string result;
        store.ReadBlock(1, 0, 0, result);
        assert(result == "block_1_data" && "Block 1 unchanged");
        
        store.ReadBlock(2, 0, 0, result);
        assert(result == "block_2_updated" && "Block 2 updated");
        
        store.ReadBlock(3, 0, 0, result);
        assert(result == "block_3_data" && "Block 3 unchanged");
        
        store.ReadBlock(4, 0, 0, result);
        assert(result == "block_4_updated" && "Block 4 updated");
        
        std::cout << "  Verified all updates" << std::endl;
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 8: Delete block
// ============================================================================
void test_delete_block() {
    std::cout << "\n=== Test 8: Delete block ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_delete";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write a block
        store.WriteBlock(1, 0, "to_be_deleted", true);
        assert(store.BlockFileExists(1) && "Block should exist");
        std::cout << "  Wrote block 1" << std::endl;
        
        // Delete it
        bool success = store.DeleteBlock(1);
        assert(success && "DeleteBlock should succeed");
        std::cout << "  Deleted block 1" << std::endl;
        
        // Verify it's gone
        assert(!store.BlockFileExists(1) && "Block should not exist after delete");
        assert(!fs::exists(get_block_path(test_dir, 1)) && "File should be deleted from disk");
        
        // Read should fail
        std::string result;
        success = store.ReadBlock(1, 0, 0, result);
        assert(!success && "ReadBlock should fail for deleted block");
        std::cout << "  Verified block 1 is deleted" << std::endl;
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 9: Block size tracking
// ============================================================================
void test_block_size() {
    std::cout << "\n=== Test 9: Block size tracking ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_size";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write block with known size
        std::string data(1000, 'X');  // 1000 bytes
        store.WriteBlock(1, 0, data, true);
        
        uint32_t size = store.GetBlockFileSize(1);
        std::cout << "  Block 1 size: " << size << " bytes" << std::endl;
        assert(size == 1000 && "Block size should be 1000");
        
        // Extend block with partial write
        store.WriteBlock(1, 1000, std::string(500, 'Y'), true);  // Append 500 bytes
        
        size = store.GetBlockFileSize(1);
        std::cout << "  After extension: " << size << " bytes" << std::endl;
        assert(size == 1500 && "Block size should be 1500 after extension");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 10: Concurrent-like access pattern (sequential simulation)
// ============================================================================
void test_access_pattern() {
    std::cout << "\n=== Test 10: Access pattern simulation ===" << std::endl;
    
    std::string test_dir = "/tmp/test_block_store_pattern";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Simulate file write: write blocks sequentially
        std::cout << "  Simulating file write (10 blocks)..." << std::endl;
        for (uint64_t i = 0; i < 10; i++) {
            std::string chunk = "chunk_" + std::to_string(i) + "_" + std::string(100, 'A' + i);
            store.WriteBlock(i, 0, chunk, false);  // No sync for performance
        }
        
        // Simulate file read: read all blocks
        std::cout << "  Simulating file read (10 blocks)..." << std::endl;
        for (uint64_t i = 0; i < 10; i++) {
            std::string result;
            store.ReadBlock(i, 0, 0, result);
            assert(result.substr(0, 8) == "chunk_" + std::to_string(i) && "Chunk header should match");
        }
        
        // Simulate random access: read specific parts
        std::cout << "  Simulating random access..." << std::endl;
        std::string result;
        store.ReadBlock(5, 8, 10, result);  // Read 10 bytes from offset 8 of block 5
        std::cout << "  Block 5, offset 8, 10 bytes: '" << result.substr(0, 10) << "...'" << std::endl;
        
        // Simulate update: modify middle of a block
        std::cout << "  Simulating partial update..." << std::endl;
        store.WriteBlock(3, 50, "MODIFIED", true);
        store.ReadBlock(3, 45, 20, result);
        std::cout << "  Block 3, bytes 45-65: '" << result << "'" << std::endl;
        assert(result.find("MODIFIED") != std::string::npos && "Should contain MODIFIED");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Main
// ============================================================================
int main() {
    std::cout << "\n======================================" << std::endl;
    std::cout << "  BlockStore Read/Write/Cache Tests" << std::endl;
    std::cout << "======================================" << std::endl;
    
    try {
        test_basic_write_read();
        test_partial_write();
        test_partial_read();
        test_cache_behavior();
        test_writeback_on_destruction();
        test_sync_write();
        test_multiple_blocks();
        test_delete_block();
        test_block_size();
        test_access_pattern();
        
        std::cout << "\n======================================" << std::endl;
        std::cout << "  All 10 tests PASSED!" << std::endl;
        std::cout << "======================================\n" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n!!! TEST FAILED with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "\n!!! TEST FAILED with unknown exception" << std::endl;
        return 1;
    }
}
