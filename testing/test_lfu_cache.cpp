#include "fs_server/lfu_cache.hpp"
#include <iostream>
#include <cassert>
#include <string>
#include <vector>

using namespace fs_server;

// Test utilities
int test_count = 0;
int passed_count = 0;

void assert_test(bool condition, const std::string& test_name) {
    test_count++;
    if (condition) {
        std::cout << "✓ PASS: " << test_name << std::endl;
        passed_count++;
    } else {
        std::cout << "✗ FAIL: " << test_name << std::endl;
    }
}

// ===========================================================================
// Basic Operations Tests
// ===========================================================================

// Test 1: Basic Put and Get
void test_basic_put_get() {
    std::cout << "\n--- Test 1: Basic Put and Get ---\n";
    LFUCache cache(10);
    std::string out_data;
    std::string test_data = "Hello World Block Data";
    
    bool put_result = cache.Put(100, test_data);
    assert_test(put_result == true, "Test 1.1: Put should return true");
    
    bool get_result = cache.Get(100, out_data);
    assert_test(get_result == true, "Test 1.2: Get existing block should return true");
    assert_test(out_data == test_data, "Test 1.3: Retrieved data should match inserted data");
}

// Test 2: Get non-existent block
void test_get_nonexistent() {
    std::cout << "\n--- Test 2: Get Non-existent Block ---\n";
    LFUCache cache(10);
    std::string out_data;
    
    bool get_result = cache.Get(999, out_data);
    assert_test(get_result == false, "Test 2: Get non-existent block should return false");
}

// Test 3: Contains check
void test_contains() {
    std::cout << "\n--- Test 3: Contains Check ---\n";
    LFUCache cache(10);
    std::string test_data = "Test Data";
    
    cache.Put(100, test_data);
    assert_test(cache.Contains(100) == true, "Test 3.1: Contains should return true for existing block");
    assert_test(cache.Contains(999) == false, "Test 3.2: Contains should return false for non-existent block");
}

// Test 4: Remove existing block
void test_remove() {
    std::cout << "\n--- Test 4: Remove Block ---\n";
    LFUCache cache(10);
    std::string test_data = "Test Data";
    
    cache.Put(100, test_data);
    assert_test(cache.Contains(100) == true, "Test 4.1: Block should exist after put");
    
    bool remove_result = cache.Remove(100);
    assert_test(remove_result == true, "Test 4.2: Remove should return true");
    assert_test(cache.Contains(100) == false, "Test 4.3: Block should not exist after remove");
}

// Test 5: Remove non-existent block
void test_remove_nonexistent() {
    std::cout << "\n--- Test 5: Remove Non-existent Block ---\n";
    LFUCache cache(10);
    
    bool remove_result = cache.Remove(999);
    assert_test(remove_result == false, "Test 5: Remove non-existent block should return false");
}

// Test 6: Update existing block
void test_update_block() {
    std::cout << "\n--- Test 6: Update Existing Block ---\n";
    LFUCache cache(10);
    std::string data1 = "First Data";
    std::string data2 = "Second Data (Updated)";
    std::string out_data;
    
    cache.Put(100, data1);
    cache.Put(100, data2);  // Update same block_uuid
    
    cache.Get(100, out_data);
    assert_test(out_data == data2, "Test 6: Updated block data should match new data");
}

// ===========================================================================
// LFU-Specific Eviction Tests
// ===========================================================================

// Test 7: Simple LFU eviction - evict least frequently used
void test_simple_lfu_eviction() {
    std::cout << "\n--- Test 7: Simple LFU Eviction ---\n";
    LFUCache cache(2);  // Capacity of 2 blocks
    std::string out_data;
    
    cache.Put(100, "Block A");
    cache.Put(101, "Block B");
    
    // Access block 100 twice more (total freq = 3: 1 put + 2 gets)
    cache.Get(100, out_data);
    cache.Get(100, out_data);
    
    // Block 101 has freq = 1 (only put)
    // Block 100 has freq = 3 (put + 2 gets)
    
    // Add block 102 - should evict block 101 (least frequently used)
    cache.Put(102, "Block C");
    
    assert_test(cache.Contains(100) == true, "Test 7.1: Block 100 should still be in cache (high freq)");
    assert_test(cache.Contains(101) == false, "Test 7.2: Block 101 should be evicted (lowest freq)");
    assert_test(cache.Contains(102) == true, "Test 7.3: Block 102 should be in cache");
}

// Test 8: LFU with same frequency - use LRU as tiebreaker
void test_lfu_same_frequency_tiebreaker() {
    std::cout << "\n--- Test 8: LFU Same Frequency Tiebreaker (LRU) ---\n";
    LFUCache cache(2);
    std::string out_data;
    
    // Both blocks added with same frequency (1)
    cache.Put(100, "Block A");  // freq = 1
    cache.Put(101, "Block B");  // freq = 1
    
    // Add block 102 - should evict block 100 (first in, same freq = LRU)
    cache.Put(102, "Block C");
    
    assert_test(cache.Contains(100) == false, "Test 8.1: Block 100 should be evicted (LRU tiebreaker)");
    assert_test(cache.Contains(101) == true, "Test 8.2: Block 101 should remain");
    assert_test(cache.Contains(102) == true, "Test 8.3: Block 102 should be in cache");
}

// Test 9: Frequency increases on Get
void test_frequency_increase_on_get() {
    std::cout << "\n--- Test 9: Frequency Increases on Get ---\n";
    LFUCache cache(3);
    std::string out_data;
    
    cache.Put(100, "Block A");  // freq = 1
    cache.Put(101, "Block B");  // freq = 1
    cache.Put(102, "Block C");  // freq = 1
    
    // Access block 100 once
    cache.Get(100, out_data);  // freq = 2
    
    // Access block 101 twice
    cache.Get(101, out_data);  // freq = 2
    cache.Get(101, out_data);  // freq = 3
    
    // Block frequencies: 100=2, 101=3, 102=1
    // Adding new block should evict 102 (freq = 1)
    cache.Put(103, "Block D");
    
    assert_test(cache.Contains(102) == false, "Test 9.1: Block 102 should be evicted (lowest freq)");
    assert_test(cache.Contains(100) == true, "Test 9.2: Block 100 should remain (freq = 2)");
    assert_test(cache.Contains(101) == true, "Test 9.3: Block 101 should remain (freq = 3)");
    assert_test(cache.Contains(103) == true, "Test 9.4: Block 103 should be in cache");
}

// Test 10: Frequency increases on Put (update)
void test_frequency_increase_on_put_update() {
    std::cout << "\n--- Test 10: Frequency Increases on Put Update ---\n";
    LFUCache cache(2);
    std::string out_data;
    
    cache.Put(100, "Block A v1");  // freq = 1
    cache.Put(101, "Block B");     // freq = 1
    
    // Update block 100 - should increase frequency
    cache.Put(100, "Block A v2");  // freq = 2
    
    // Add new block - should evict 101 (freq = 1), not 100 (freq = 2)
    cache.Put(102, "Block C");
    
    assert_test(cache.Contains(100) == true, "Test 10.1: Block 100 should remain (freq = 2)");
    assert_test(cache.Contains(101) == false, "Test 10.2: Block 101 should be evicted (freq = 1)");
    assert_test(cache.Contains(102) == true, "Test 10.3: Block 102 should be in cache");
    
    // Verify updated data
    cache.Get(100, out_data);
    assert_test(out_data == "Block A v2", "Test 10.4: Block 100 should have updated data");
}

// Test 11: New blocks always have frequency = 1
void test_new_blocks_frequency_reset() {
    std::cout << "\n--- Test 11: New Blocks Always Start with Frequency 1 ---\n";
    LFUCache cache(2);
    std::string out_data;
    
    cache.Put(100, "Block A");
    cache.Get(100, out_data);  // freq = 2
    cache.Get(100, out_data);  // freq = 3
    cache.Get(100, out_data);  // freq = 4
    
    cache.Put(101, "Block B");  // freq = 1
    
    // Add new block - should evict 101 (freq = 1), not 100 (freq = 4)
    cache.Put(102, "Block C");
    
    assert_test(cache.Contains(100) == true, "Test 11.1: Block 100 should remain (high freq)");
    assert_test(cache.Contains(101) == false, "Test 11.2: Block 101 should be evicted (new, freq = 1)");
    assert_test(cache.Contains(102) == true, "Test 11.3: Block 102 should be in cache");
}

// Test 12: Multiple evictions with varying frequencies
void test_multiple_evictions_varying_frequencies() {
    std::cout << "\n--- Test 12: Multiple Evictions with Varying Frequencies ---\n";
    LFUCache cache(3);
    std::string out_data;
    
    cache.Put(100, "A");  // freq = 1
    cache.Put(101, "B");  // freq = 1
    cache.Put(102, "C");  // freq = 1
    
    // Create different frequencies
    cache.Get(100, out_data);  // 100: freq = 2
    cache.Get(100, out_data);  // 100: freq = 3
    cache.Get(100, out_data);  // 100: freq = 4
    
    cache.Get(101, out_data);  // 101: freq = 2
    
    // 102 still has freq = 1
    // Frequencies: 100=4, 101=2, 102=1
    
    // Add 103 - evicts 102 (freq = 1)
    cache.Put(103, "D");
    assert_test(cache.Contains(102) == false, "Test 12.1: Block 102 evicted (freq=1)");
    
    // Add 104 - evicts 103 (freq = 1, newer than 101 with freq = 2)
    cache.Put(104, "E");
    assert_test(cache.Contains(103) == false, "Test 12.2: Block 103 evicted (freq=1)");
    
    // Add 105 - evicts 104 (freq = 1)
    cache.Put(105, "F");
    assert_test(cache.Contains(104) == false, "Test 12.3: Block 104 evicted (freq=1)");
    
    // High frequency blocks should still be there
    assert_test(cache.Contains(100) == true, "Test 12.4: Block 100 remains (freq=4)");
    assert_test(cache.Contains(101) == true, "Test 12.5: Block 101 remains (freq=2)");
}

// Test 13: Eviction with many blocks at same frequency
void test_eviction_many_same_frequency() {
    std::cout << "\n--- Test 13: Eviction with Many Blocks at Same Frequency ---\n";
    LFUCache cache(3);
    
    // All blocks have same frequency (1)
    cache.Put(100, "A");
    cache.Put(101, "B");
    cache.Put(102, "C");
    
    // Add more blocks - oldest should be evicted first (LRU tiebreaker)
    cache.Put(103, "D");  // Evicts 100
    assert_test(cache.Contains(100) == false, "Test 13.1: Block 100 evicted first");
    
    cache.Put(104, "E");  // Evicts 101
    assert_test(cache.Contains(101) == false, "Test 13.2: Block 101 evicted second");
    
    cache.Put(105, "F");  // Evicts 102
    assert_test(cache.Contains(102) == false, "Test 13.3: Block 102 evicted third");
    
    // Latest three should remain
    assert_test(cache.Contains(103) == true, "Test 13.4: Block 103 remains");
    assert_test(cache.Contains(104) == true, "Test 13.5: Block 104 remains");
    assert_test(cache.Contains(105) == true, "Test 13.6: Block 105 remains");
}

// ===========================================================================
// Edge Cases
// ===========================================================================

// Test 14: Single capacity cache
void test_single_capacity() {
    std::cout << "\n--- Test 14: Single Capacity Cache ---\n";
    LFUCache cache(1);
    std::string out_data;
    
    cache.Put(100, "A");
    assert_test(cache.Contains(100) == true, "Test 14.1: Single block in cache");
    
    cache.Put(101, "B");  // Evicts 100
    assert_test(cache.Contains(100) == false, "Test 14.2: Block 100 evicted");
    assert_test(cache.Contains(101) == true, "Test 14.3: Block 101 in cache");
    
    // Even with high frequency, new block evicts old
    cache.Get(101, out_data);  // freq = 2
    cache.Get(101, out_data);  // freq = 3
    
    cache.Put(102, "C");  // Evicts 101 despite high freq
    assert_test(cache.Contains(101) == false, "Test 14.4: Block 101 evicted (only block)");
    assert_test(cache.Contains(102) == true, "Test 14.5: Block 102 in cache");
}

// Test 15: Empty string data
void test_empty_string() {
    std::cout << "\n--- Test 15: Empty String Data ---\n";
    LFUCache cache(10);
    std::string out_data;
    
    bool put_result = cache.Put(100, "");
    assert_test(put_result == true, "Test 15.1: Put empty string should succeed");
    assert_test(cache.Contains(100) == true, "Test 15.2: Block with empty data should be in cache");
    
    bool get_result = cache.Get(100, out_data);
    assert_test(get_result == true, "Test 15.3: Get should succeed");
    assert_test(out_data == "", "Test 15.4: Retrieved data should be empty string");
}

// Test 16: Large block data
void test_large_block_data() {
    std::cout << "\n--- Test 16: Large Block Data ---\n";
    LFUCache cache(10);
    std::string out_data;
    std::string large_data(100000, 'X');  // 100KB
    
    cache.Put(100, large_data);
    bool get_result = cache.Get(100, out_data);
    
    assert_test(get_result == true, "Test 16.1: Get should succeed");
    assert_test(out_data == large_data, "Test 16.2: Large data should match");
}

// Test 17: Clear cache
void test_clear() {
    std::cout << "\n--- Test 17: Clear Cache ---\n";
    LFUCache cache(10);
    
    cache.Put(100, "A");
    cache.Put(101, "B");
    cache.Put(102, "C");
    
    assert_test(cache.Contains(100) == true, "Test 17.1: Cache has blocks before clear");
    
    cache.Clear();
    
    assert_test(cache.Contains(100) == false, "Test 17.2: Block 100 gone after clear");
    assert_test(cache.Contains(101) == false, "Test 17.3: Block 101 gone after clear");
    assert_test(cache.Contains(102) == false, "Test 17.4: Block 102 gone after clear");
    
    // Should be able to add blocks again
    cache.Put(200, "New");
    assert_test(cache.Contains(200) == true, "Test 17.5: Can add blocks after clear");
}

// ===========================================================================
// Statistics Tests
// ===========================================================================

// Test 18: Cache stats - hits and misses
void test_stats_hits_misses() {
    std::cout << "\n--- Test 18: Cache Stats - Hits and Misses ---\n";
    LFUCache cache(10);
    std::string out_data;
    
    cache.Put(100, "Test Data");
    
    // Cache hits
    cache.Get(100, out_data);
    cache.Get(100, out_data);
    
    // Cache misses
    cache.Get(999, out_data);
    cache.Get(998, out_data);
    cache.Get(997, out_data);
    
    auto stats = cache.GetStats();
    assert_test(stats.hits == 2, "Test 18.1: Stats should show 2 hits");
    assert_test(stats.misses == 3, "Test 18.2: Stats should show 3 misses");
    assert_test(stats.policy_name == "LFU", "Test 18.3: Policy name should be LFU");
}

// Test 19: Cache stats - evictions
void test_stats_evictions() {
    std::cout << "\n--- Test 19: Cache Stats - Evictions ---\n";
    LFUCache cache(2);
    
    cache.Put(100, "A");
    cache.Put(101, "B");
    cache.Put(102, "C");  // 1 eviction
    cache.Put(103, "D");  // 2 evictions
    cache.Put(104, "E");  // 3 evictions
    
    auto stats = cache.GetStats();
    assert_test(stats.evictions == 3, "Test 19: Stats should show 3 evictions");
}

// Test 20: Reset stats
void test_reset_stats() {
    std::cout << "\n--- Test 20: Reset Stats ---\n";
    LFUCache cache(2);
    std::string out_data;
    
    cache.Put(100, "A");
    cache.Get(100, out_data);
    cache.Get(999, out_data);
    cache.Put(101, "B");
    cache.Put(102, "C");  // eviction
    
    cache.ResetStats();
    
    auto stats = cache.GetStats();
    assert_test(stats.hits == 0, "Test 20.1: Hits should be reset to 0");
    assert_test(stats.misses == 0, "Test 20.2: Misses should be reset to 0");
    assert_test(stats.evictions == 0, "Test 20.3: Evictions should be reset to 0");
}

// ===========================================================================
// Eviction Callback Tests
// ===========================================================================

// Test 21: Eviction callback is called
void test_eviction_callback() {
    std::cout << "\n--- Test 21: Eviction Callback ---\n";
    LFUCache cache(2);
    
    uint64_t evicted_block = 0;
    std::string evicted_data;
    
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        evicted_block = block_uuid;
        evicted_data = data;
    });
    
    cache.Put(100, "Block A", true);  // dirty = true
    cache.Put(101, "Block B", true);
    cache.Put(102, "Block C", true);  // triggers eviction of block 100
    
    assert_test(evicted_block == 100, "Test 21.1: Eviction callback should be called with block 100");
    assert_test(evicted_data == "Block A", "Test 21.2: Eviction callback should have correct data");
}

// Test 22: Eviction callback only for dirty blocks
void test_eviction_callback_dirty_only() {
    std::cout << "\n--- Test 22: Eviction Callback for Dirty Blocks Only ---\n";
    LFUCache cache(2);
    
    int callback_count = 0;
    
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        callback_count++;
    });
    
    cache.Put(100, "Block A", false);  // dirty = false
    cache.Put(101, "Block B", false);  // dirty = false
    cache.Put(102, "Block C", false);  // triggers eviction, but block 100 is clean
    
    assert_test(callback_count == 0, "Test 22: Callback should not be called for clean blocks");
}

// Test 23: FlushAll calls callback for dirty blocks
void test_flush_all() {
    std::cout << "\n--- Test 23: FlushAll ---\n";
    LFUCache cache(10);
    
    std::vector<uint64_t> flushed_blocks;
    
    cache.SetEvictionCallback([&](uint64_t block_uuid, const std::string& data) {
        flushed_blocks.push_back(block_uuid);
    });
    
    cache.Put(100, "A", true);   // dirty
    cache.Put(101, "B", false);  // clean
    cache.Put(102, "C", true);   // dirty
    cache.Put(103, "D", true);   // dirty
    
    cache.FlushAll();
    
    assert_test(flushed_blocks.size() == 3, "Test 23.1: FlushAll should flush 3 dirty blocks");
    
    // Blocks should still be in cache after flush
    assert_test(cache.Contains(100) == true, "Test 23.2: Block 100 still in cache after flush");
    assert_test(cache.Contains(101) == true, "Test 23.3: Block 101 still in cache after flush");
    assert_test(cache.Contains(102) == true, "Test 23.4: Block 102 still in cache after flush");
    assert_test(cache.Contains(103) == true, "Test 23.5: Block 103 still in cache after flush");
}

// ===========================================================================
// Complex Scenario Tests
// ===========================================================================

// Test 24: Complex mixed operations
void test_complex_scenario() {
    std::cout << "\n--- Test 24: Complex Mixed Operations ---\n";
    LFUCache cache(3);
    std::string out_data;
    
    // Initial insertions
    cache.Put(100, "A");  // freq = 1
    cache.Put(101, "B");  // freq = 1
    cache.Put(102, "C");  // freq = 1
    
    // Increase frequencies
    cache.Get(100, out_data);  // 100: freq = 2
    cache.Get(101, out_data);  // 101: freq = 2
    cache.Get(100, out_data);  // 100: freq = 3
    
    // Remove one block
    cache.Remove(101);
    assert_test(cache.Contains(101) == false, "Test 24.1: Block 101 removed");
    
    // Add new block
    cache.Put(103, "D");  // freq = 1
    
    // Frequencies: 100=3, 102=1, 103=1
    // Add 104 - should evict 102 or 103 (both freq=1, 102 is older)
    cache.Put(104, "E");
    assert_test(cache.Contains(102) == false, "Test 24.2: Block 102 evicted (older with freq=1)");
    
    // 100 should still be there (high freq)
    assert_test(cache.Contains(100) == true, "Test 24.3: Block 100 remains (freq=3)");
}

// Test 25: Stress test with many operations
void test_stress() {
    std::cout << "\n--- Test 25: Stress Test ---\n";
    LFUCache cache(100);
    std::string out_data;
    
    // Insert 200 blocks (causes evictions)
    for (int i = 0; i < 200; i++) {
        cache.Put(i, "Block " + std::to_string(i));
    }
    
    // Access some blocks to increase frequency
    for (int i = 100; i < 200; i++) {
        cache.Get(i, out_data);
    }
    
    // Insert more blocks
    for (int i = 200; i < 300; i++) {
        cache.Put(i, "Block " + std::to_string(i));
    }
    
    auto stats = cache.GetStats();
    assert_test(stats.evictions > 0, "Test 25.1: Should have evictions");
    assert_test(stats.hits > 0, "Test 25.2: Should have hits");
    
    // Verify cache is still functional
    cache.Put(999, "Final Block");
    bool get_result = cache.Get(999, out_data);
    assert_test(get_result == true, "Test 25.3: Cache still functional after stress");
    assert_test(out_data == "Final Block", "Test 25.4: Data integrity maintained");
}

// Test 26: Verify min_freq is reset after eviction
void test_min_freq_reset() {
    std::cout << "\n--- Test 26: Min Frequency Reset After Eviction ---\n";
    LFUCache cache(2);
    std::string out_data;
    
    cache.Put(100, "A");  // freq = 1
    cache.Get(100, out_data);  // freq = 2
    cache.Get(100, out_data);  // freq = 3
    
    cache.Put(101, "B");  // freq = 1
    cache.Get(101, out_data);  // freq = 2
    
    // min_freq should be 2 now
    // Add new block - min_freq resets to 1
    cache.Put(102, "C");  // freq = 1, evicts 101 (lower freq than 100)
    
    assert_test(cache.Contains(100) == true, "Test 26.1: Block 100 remains (freq=3)");
    assert_test(cache.Contains(101) == false, "Test 26.2: Block 101 evicted (freq=2)");
    assert_test(cache.Contains(102) == true, "Test 26.3: Block 102 in cache (freq=1)");
    
    // Add another block - should evict 102 (freq=1)
    cache.Put(103, "D");
    assert_test(cache.Contains(102) == false, "Test 26.4: Block 102 evicted (freq=1)");
    assert_test(cache.Contains(103) == true, "Test 26.5: Block 103 in cache");
}

// ===========================================================================
// Main
// ===========================================================================

int main() {
    std::cout << "=== LFU Cache Test Suite ===" << std::endl;
    
    // Basic Operations
    test_basic_put_get();
    test_get_nonexistent();
    test_contains();
    test_remove();
    test_remove_nonexistent();
    test_update_block();
    
    // LFU-Specific Eviction Tests
    test_simple_lfu_eviction();
    test_lfu_same_frequency_tiebreaker();
    test_frequency_increase_on_get();
    test_frequency_increase_on_put_update();
    test_new_blocks_frequency_reset();
    test_multiple_evictions_varying_frequencies();
    test_eviction_many_same_frequency();
    
    // Edge Cases
    test_single_capacity();
    test_empty_string();
    test_large_block_data();
    test_clear();
    
    // Statistics Tests
    test_stats_hits_misses();
    test_stats_evictions();
    test_reset_stats();
    
    // Eviction Callback Tests
    test_eviction_callback();
    test_eviction_callback_dirty_only();
    test_flush_all();
    
    // Complex Scenarios
    test_complex_scenario();
    test_stress();
    test_min_freq_reset();
    
    std::cout << std::endl << "=== Test Results ===" << std::endl;
    std::cout << "Passed: " << passed_count << " / " << test_count << std::endl;
    
    if (passed_count == test_count) {
        std::cout << "All tests passed! ✓" << std::endl;
        return 0;
    } else {
        std::cout << "Some tests failed! ✗" << std::endl;
        return 1;
    }
}
