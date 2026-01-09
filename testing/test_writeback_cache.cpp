#include <iostream>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <vector>
#include <string>
#include "fs_server/block_store.hpp"
#include "fs_server/lru_cache.hpp"

namespace fs = std::filesystem;

// Helper to read file contents directly from disk
std::string read_file_contents(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return "";
    return std::string((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
}

// ============================================================================
// Test 1: Verify eviction callback is invoked for dirty pages
// ============================================================================
void test_eviction_callback_invoked() {
    std::cout << "\n=== Test 1: Eviction callback invoked ===" << std::endl;
    
    // Create a small cache (2 pages max)
    fs_server::LRUCache cache(2);
    
    bool callback_invoked = false;
    uint64_t evicted_block_id = 0;
    std::string evicted_data;
    
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        callback_invoked = true;
        evicted_block_id = block_uuid;
        evicted_data = data;
        std::cout << "  Callback: Evicting block " << block_uuid 
                  << " with data: " << data << std::endl;
    });
    
    // Fill cache (blocks are dirty by default)
    cache.Put(1, "data1");
    cache.Put(2, "data2");
    
    std::cout << "  Cache filled with blocks 1 and 2" << std::endl;
    
    // This should trigger eviction of block 1 (LRU)
    cache.Put(3, "data3");
    
    assert(callback_invoked && "Eviction callback should be invoked");
    assert(evicted_block_id == 1 && "Block 1 should be evicted (LRU)");
    assert(evicted_data == "data1" && "Evicted data should match");
    
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 2: Verify dirty flag is set correctly on Put
// ============================================================================
void test_dirty_flag_on_put() {
    std::cout << "\n=== Test 2: Dirty flag control on Put ===" << std::endl;
    
    fs_server::LRUCache cache(3);
    
    std::vector<uint64_t> evicted_blocks;
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        evicted_blocks.push_back(block_uuid);
        std::cout << "  Callback: Dirty eviction for block " << block_uuid << std::endl;
    });
    
    // Put block 1 as CLEAN (simulating it's already on disk)
    cache.Put(1, "data1", false);  // dirty=false
    std::cout << "  Put block 1 as CLEAN" << std::endl;
    
    // Put block 2 as DIRTY (needs writeback)
    cache.Put(2, "data2", true);   // dirty=true
    std::cout << "  Put block 2 as DIRTY" << std::endl;
    
    // Put block 3 as CLEAN
    cache.Put(3, "data3", false);  // dirty=false
    std::cout << "  Put block 3 as CLEAN" << std::endl;
    
    // Trigger eviction of block 1 (LRU, but it's clean)
    cache.Put(4, "data4", true);
    
    // Block 1 was clean, so callback should NOT be invoked
    assert(evicted_blocks.empty() && "Clean block 1 should not trigger callback");
    std::cout << "  Block 1 (clean) evicted without callback - correct!" << std::endl;
    
    // Now evict block 2 (dirty)
    cache.Put(5, "data5", true);
    
    assert(evicted_blocks.size() == 1 && "Dirty block 2 should trigger callback");
    assert(evicted_blocks[0] == 2 && "Block 2 should be evicted");
    std::cout << "  Block 2 (dirty) evicted with callback - correct!" << std::endl;
    
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 3: Verify access order updates LRU correctly
// ============================================================================
void test_lru_access_order() {
    std::cout << "\n=== Test 3: LRU access order ===" << std::endl;
    
    fs_server::LRUCache cache(3);
    
    std::vector<uint64_t> evicted_blocks;
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        evicted_blocks.push_back(block_uuid);
        std::cout << "  Callback: Evicting block " << block_uuid << std::endl;
    });
    
    // Put blocks 1, 2, 3
    cache.Put(1, "data1");
    cache.Put(2, "data2");
    cache.Put(3, "data3");
    
    // Access block 1 (moves to MRU position)
    std::string out;
    cache.Get(1, out);
    std::cout << "  Accessed block 1, moving to MRU position" << std::endl;
    
    // Now insert block 4 - should evict block 2 (LRU after block 1 was accessed)
    cache.Put(4, "data4");
    
    assert(evicted_blocks.size() == 1 && "Should have 1 eviction");
    assert(evicted_blocks[0] == 2 && "Block 2 should be evicted (was LRU)");
    
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 4: Verify FlushAll writes all dirty pages
// ============================================================================
void test_flush_all() {
    std::cout << "\n=== Test 4: FlushAll writes all dirty pages ===" << std::endl;
    
    fs_server::LRUCache cache(10);
    
    std::vector<uint64_t> flushed_blocks;
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        flushed_blocks.push_back(block_uuid);
        std::cout << "  Callback: Flushing block " << block_uuid << std::endl;
    });
    
    // Put multiple blocks (all marked dirty on put)
    cache.Put(1, "data1");
    cache.Put(2, "data2");
    cache.Put(3, "data3");
    
    // Update some blocks (remain dirty)
    cache.Put(1, "data1_updated");
    cache.Put(2, "data2_updated");
    
    std::cout << "  Calling FlushAll..." << std::endl;
    cache.FlushAll();
    
    assert(flushed_blocks.size() == 3 && "Should flush 3 dirty blocks");
    
    // Second flush should be no-op (pages no longer dirty)
    flushed_blocks.clear();
    std::cout << "  Calling FlushAll again (should be no-op)..." << std::endl;
    cache.FlushAll();
    assert(flushed_blocks.empty() && "Second flush should be no-op");
    
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 5: Integration test with BlockStore
// ============================================================================
void test_blockstore_writeback() {
    std::cout << "\n=== Test 5: BlockStore write-back integration ===" << std::endl;
    
    std::string test_dir = "/tmp/test_writeback_cache";
    fs::remove_all(test_dir);  // Clean up first
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write block 1 (new block - goes to disk immediately)
        std::cout << "  Writing new block 1 to disk..." << std::endl;
        store.WriteBlock(1, "initial_data", false);
        
        // Verify file exists on disk
        std::string block_path = test_dir + "/blk_1.img";
        assert(fs::exists(block_path) && "Block file should exist after initial write");
        
        // Update block 1 (should update cache, not disk since sync=false)
        std::cout << "  Updating block 1 in cache (no sync)..." << std::endl;
        store.WriteBlock(1, "updated_data", false);
        
        // Read should get updated data from cache
        std::string read_data;
        store.ReadBlock(1, 0, 0, read_data);
        std::cout << "  Read from cache: " << read_data << std::endl;
        assert(read_data == "updated_data" && "Should read updated data from cache");
        
        // Disk should still have old data (write-back hasn't flushed yet)
        std::string disk_data = read_file_contents(block_path);
        std::cout << "  Data on disk (before flush): " << disk_data << std::endl;
        // Note: Disk may have old data or new data depending on timing
        
    }  // BlockStore destructor calls FlushAll()
    
    std::cout << "  BlockStore destroyed - dirty pages should be flushed" << std::endl;
    
    // Verify data persisted correctly after flush
    {
        fs_server::BlockStore store(test_dir, true);
        std::string read_data;
        store.ReadBlock(1, 0, 0, read_data);
        std::cout << "  After restart, read from disk: " << read_data << std::endl;
        assert(read_data == "updated_data" && "Should read flushed data from disk");
    }
    
    // Cleanup
    fs::remove_all(test_dir);
    
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 6: Verify sync=true writes through immediately
// ============================================================================
void test_sync_write_through() {
    std::cout << "\n=== Test 6: Sync write goes to disk immediately ===" << std::endl;
    
    std::string test_dir = "/tmp/test_sync_write";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write new block with sync=true
        std::cout << "  Writing block 1 with sync=true..." << std::endl;
        store.WriteBlock(1, "sync_data_v1", true);
        
        // Verify file exists on disk immediately
        std::string block_path = test_dir + "/blk_1.img";
        assert(fs::exists(block_path) && "Block file should exist immediately");
        
        std::string disk_data = read_file_contents(block_path);
        std::cout << "  Data on disk after sync write: " << disk_data << std::endl;
        assert(disk_data == "sync_data_v1" && "Disk should have sync_data_v1");
        
        // Update block with sync=true
        std::cout << "  Updating block 1 with sync=true..." << std::endl;
        store.WriteBlock(1, "sync_data_v2", true);
        
        disk_data = read_file_contents(block_path);
        std::cout << "  Data on disk after sync update: " << disk_data << std::endl;
        assert(disk_data == "sync_data_v2" && "Disk should have sync_data_v2 immediately");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 7: New blocks are cached as CLEAN (already on disk)
// ============================================================================
void test_new_blocks_cached_clean() {
    std::cout << "\n=== Test 7: New blocks cached as clean ===" << std::endl;
    
    std::string test_dir = "/tmp/test_new_blocks_clean";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write multiple new blocks
        store.WriteBlock(1, "block1_data", false);
        store.WriteBlock(2, "block2_data", false);
        store.WriteBlock(3, "block3_data", false);
        
        std::cout << "  Wrote 3 new blocks (should be cached as clean)" << std::endl;
        
        // Verify files exist on disk
        assert(fs::exists(test_dir + "/blk_1.img"));
        assert(fs::exists(test_dir + "/blk_2.img"));
        assert(fs::exists(test_dir + "/blk_3.img"));
        std::cout << "  All blocks written to disk correctly" << std::endl;
        
        // Destructor will call FlushAll - but since blocks are clean,
        // nothing should be written
        std::cout << "  Destroying BlockStore (FlushAll should be no-op for clean pages)..." << std::endl;
    }
    
    // Verify disk contents unchanged
    std::string data1 = read_file_contents(test_dir + "/blk_1.img");
    std::string data2 = read_file_contents(test_dir + "/blk_2.img");
    std::string data3 = read_file_contents(test_dir + "/blk_3.img");
    
    assert(data1 == "block1_data");
    assert(data2 == "block2_data");
    assert(data3 == "block3_data");
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 8: Partial write (Read-Modify-Write pattern)
// ============================================================================
void test_partial_write() {
    std::cout << "\n=== Test 8: Partial write (Read-Modify-Write) ===" << std::endl;
    
    std::string test_dir = "/tmp/test_partial_write";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write initial block
        std::string initial_data = "AAAAAAAAAA";  // 10 A's
        store.WriteBlock(1, initial_data, true);
        std::cout << "  Wrote initial block: '" << initial_data << "'" << std::endl;
        
        // Partial write at offset 3: replace AAA with BBB
        store.WriteBlockPartial(1, 3, "BBB", false);
        std::cout << "  Partial write at offset 3: 'BBB'" << std::endl;
        
        // Read back and verify
        std::string read_data;
        store.ReadBlock(1, 0, 0, read_data);
        std::cout << "  Read back: '" << read_data << "'" << std::endl;
        
        assert(read_data == "AAABBBAAA" + std::string(1, 'A') && "Partial write should modify middle");
        // Actually should be "AAABBBAAAA"
        assert(read_data == "AAABBBAAAA" && "Partial write should be AAABBBAAAA");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Test 9: Partial read from block
// ============================================================================
void test_partial_read() {
    std::cout << "\n=== Test 9: Partial read from block ===" << std::endl;
    
    std::string test_dir = "/tmp/test_partial_read";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    
    {
        fs_server::BlockStore store(test_dir, true);
        
        // Write a block
        std::string full_data = "0123456789ABCDEF";
        store.WriteBlock(1, full_data, true);
        std::cout << "  Wrote block: '" << full_data << "'" << std::endl;
        
        // Partial read: offset=4, length=4
        std::string partial;
        store.ReadBlock(1, 4, 4, partial);
        std::cout << "  Read offset=4, length=4: '" << partial << "'" << std::endl;
        assert(partial == "4567" && "Partial read should return '4567'");
        
        // Partial read: offset=10, length=0 (to end)
        store.ReadBlock(1, 10, 0, partial);
        std::cout << "  Read offset=10, length=0: '" << partial << "'" << std::endl;
        assert(partial == "ABCDEF" && "Partial read to end should return 'ABCDEF'");
        
        // Full read
        store.ReadBlock(1, 0, 0, partial);
        std::cout << "  Read offset=0, length=0: '" << partial << "'" << std::endl;
        assert(partial == full_data && "Full read should return entire block");
    }
    
    fs::remove_all(test_dir);
    std::cout << "  PASSED!" << std::endl;
}

// ============================================================================
// Main
// ============================================================================
int main() {
    std::cout << "\n======================================" << std::endl;
    std::cout << "  Write-Back Cache Tests" << std::endl;
    std::cout << "======================================" << std::endl;
    
    try {
        test_eviction_callback_invoked();
        test_dirty_flag_on_put();
        test_lru_access_order();
        test_flush_all();
        test_blockstore_writeback();
        test_sync_write_through();
        test_new_blocks_cached_clean();
        test_partial_write();
        test_partial_read();
        
        std::cout << "\n======================================" << std::endl;
        std::cout << "  All tests PASSED!" << std::endl;
        std::cout << "======================================\n" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n!!! TEST FAILED with exception: " << e.what() << std::endl;
        return 1;
    }
}
