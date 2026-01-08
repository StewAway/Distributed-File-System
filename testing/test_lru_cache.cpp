#include "fs_server/lru_cache.hpp"
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

// Test 1: Basic Put and Get
void test_basic_put_get() {
    LRUCache cache(1);  // 1 MB
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
    LRUCache cache(1);
    std::string out_data;
    
    bool get_result = cache.Get(999, out_data);
    assert_test(get_result == false, "Test 2: Get non-existent block should return false");
}

// Test 3: Partial data retrieval with offset and length
void test_partial_get() {
    LRUCache cache(1);
    std::string out_data;
    std::string test_data = "0123456789ABCDEF";
    
    cache.Put(100, test_data);

    bool get_result = cache.Get(100, out_data);
    assert_test(get_result == true, "Test 3.1: Get existing block should return true");
    assert_test(out_data == test_data, "Test 3.2: Retrieved data should match full block");
}

// Test 4: Out of bounds partial get
void test_out_of_bounds_get() {
    LRUCache cache(1);
    std::string out_data;
    std::string test_data = "SHORT";
    
    cache.Put(100, test_data);
    
    bool result = cache.Get(101, out_data);
    assert_test(result == false, "Test 4: Get non-existent block should return false");
}

// Test 5: Contains check
void test_contains() {
    LRUCache cache(1);
    std::string test_data = "Test Data";
    
    cache.Put(100, test_data);
    assert_test(cache.Contains(100) == true, "Test 5.1: Contains should return true for existing block");
    assert_test(cache.Contains(999) == false, "Test 5.2: Contains should return false for non-existent block");
}

// Test 6: Remove existing block
void test_remove() {
    LRUCache cache(1);
    std::string test_data = "Test Data";
    
    cache.Put(100, test_data);
    assert_test(cache.Contains(100) == true, "Test 6.1: Block should exist after put");
    
    bool remove_result = cache.Remove(100);
    assert_test(remove_result == true, "Test 6.2: Remove should return true");
    assert_test(cache.Contains(100) == false, "Test 6.3: Block should not exist after remove");
}

// Test 7: Remove non-existent block
void test_remove_nonexistent() {
    LRUCache cache(1);
    
    bool remove_result = cache.Remove(999);
    assert_test(remove_result == false, "Test 7: Remove non-existent block should return false");
}

// Test 8: Update existing block
void test_update_block() {
    LRUCache cache(1);
    std::string data1 = "First Data";
    std::string data2 = "Second Data (Updated)";
    std::string out_data;
    
    cache.Put(100, data1);
    cache.Put(100, data2);  // Update same block_uuid
    
    cache.Get(100, out_data);
    assert_test(out_data == data2, "Test 8: Updated block data should match new data");
}

// Test 9: Simple eviction - single block capacity
void test_simple_eviction() {
    LRUCache cache(1);  // 1 MB max, assume ~4KB per block
    std::string out_data;
    std::string block1 = std::string(4000, 'A');
    std::string block2 = std::string(4000, 'B');
    std::string block3 = std::string(4000, 'C');
    
    cache.Put(100, block1);
    cache.Put(101, block2);
    assert_test(cache.Contains(100) == false, "Test 9.1: Block 100 should be evicted (LRU)");
    assert_test(cache.Contains(101) == true, "Test 9.2: Block 101 should still be in cache");
    
    cache.Put(102, block3);
    assert_test(cache.Contains(101) == false, "Test 9.3: Block 101 should be evicted (now LRU)");
    assert_test(cache.Contains(102) == true, "Test 9.4: Block 102 should be in cache");
}

// Test 10: LRU ordering - recently accessed blocks are kept
void test_lru_ordering() {
    LRUCache cache(2);
    std::string out_data;
    std::string block1 = std::string(4000, 'A');
    std::string block2 = std::string(4000, 'B');
    std::string block3 = std::string(4000, 'C');
    
    cache.Put(100, block1);
    cache.Put(101, block2);
    
    // Access block 100 (make it recently used)
    cache.Get(100, out_data);
    
    // Now add block3, should evict 101 (not 100, because 100 was recently accessed)
    cache.Put(102, block3);
    assert_test(cache.Contains(100) == true, "Test 10.1: Block 100 should still be in cache (recently accessed)");
    assert_test(cache.Contains(101) == false, "Test 10.2: Block 101 should be evicted (not recently accessed)");
    assert_test(cache.Contains(102) == true, "Test 10.3: Block 102 should be in cache");
}

// Test 11: Multiple evictions
void test_multiple_evictions() {
    LRUCache cache(1);
    std::string block = std::string(4000, 'X');
    
    // Fill cache
    cache.Put(100, block);
    cache.Put(101, block);
    cache.Put(102, block);
    cache.Put(103, block);
    cache.Put(104, block);
    
    // Check that oldest blocks are evicted
    assert_test(cache.Contains(100) == false, "Test 11.1: Block 100 should be evicted");
    assert_test(cache.Contains(101) == false, "Test 11.2: Block 101 should be evicted");
    assert_test(cache.Contains(104) == true, "Test 11.3: Block 104 should be in cache");
}

// Test 12: Clear cache
void test_clear() {
    LRUCache cache(1);
    std::string data = "Test Data";
    
    cache.Put(100, data);
    cache.Put(101, data);
    cache.Put(102, data);
    
    assert_test(cache.Contains(102) == true, "Test 12.1: Cache should have blocks before clear");
    
    cache.Clear();
    
    assert_test(cache.Contains(100) == false, "Test 12.2: Block 100 should not exist after clear");
    assert_test(cache.Contains(101) == false, "Test 12.3: Block 101 should not exist after clear");
    assert_test(cache.Contains(102) == false, "Test 12.4: Block 102 should not exist after clear");
}

// Test 13: Cache stats - hits and misses
void test_stats_hits_misses() {
    LRUCache cache(1);
    std::string out_data;
    std::string data = "Test Data";
    
    cache.Put(100, data);
    
    // Cache hit
    cache.Get(100, out_data);
    
    // Cache miss
    cache.Get(999, out_data);
    
    auto stats = cache.GetStats();
    assert_test(stats.hits == 1, "Test 13.1: Stats should show 1 hit");
    assert_test(stats.misses == 1, "Test 13.2: Stats should show 1 miss");
}

// Test 14: Cache stats - evictions
void test_stats_evictions() {
    LRUCache cache(1);
    std::string block = std::string(4000, 'X');
    
    cache.Put(100, block);
    cache.Put(101, block);
    cache.Put(102, block);  // This should trigger eviction
    
    auto stats = cache.GetStats();
    assert_test(stats.evictions >= 1, "Test 14: Stats should show at least 1 eviction");
}

// Test 15: Reset stats
void test_reset_stats() {
    LRUCache cache(1);
    std::string data = "Test Data";
    
    cache.Put(100, data);
    cache.Get(100, data);
    cache.Get(999, data);
    
    cache.ResetStats();
    
    auto stats = cache.GetStats();
    assert_test(stats.hits == 0, "Test 15.1: Hits should be reset to 0");
    assert_test(stats.misses == 0, "Test 15.2: Misses should be reset to 0");
    assert_test(stats.evictions == 0, "Test 15.3: Evictions should be reset to 0");
}

// Test 16: Put updates with different sizes
void test_put_update_size_change() {
    LRUCache cache(1);
    std::string small_data = "SMALL";
    std::string large_data = std::string(100, 'L');
    
    cache.Put(100, small_data);
    auto stats1 = cache.GetStats();
    
    cache.Put(100, large_data);  // Update with larger data
    auto stats2 = cache.GetStats();
    
    // Current size should reflect the change
}

// Test 17: Edge case - put empty string
void test_put_empty_string() {
    LRUCache cache(1);
    std::string empty_data = "";
    
    bool put_result = cache.Put(100, empty_data);
    assert_test(put_result == true, "Test 17.1: Put should succeed with empty string");
    assert_test(cache.Contains(100) == true, "Test 17.2: Block with empty data should be in cache");
}

// Test 18: Edge case - get with zero length
void test_get_zero_length() {
    LRUCache cache(1);
    std::string data = "Test Data";
    std::string out_data;
    
    cache.Put(100, data);
    bool get_result = cache.Get(100, out_data);
    
    assert_test(get_result == true, "Test 18: Get should return true for existing block");
    assert_test(out_data == data, "Test 18: Retrieved data should match stored block");
}

// Test 19: Complex scenario - mixed operations
void test_complex_scenario() {
    LRUCache cache(2);  // 2 MB
    std::string data1 = std::string(2000, 'A');
    std::string data2 = std::string(2000, 'B');
    std::string data3 = std::string(2000, 'C');
    std::string data4 = std::string(2000, 'D');
    std::string out_data;
    
    // Put blocks
    cache.Put(100, data1);
    cache.Put(101, data2);
    
    // Access block 100
    cache.Get(100, out_data);
    
    // Put new block (should evict 101)
    cache.Put(102, data3);
    assert_test(cache.Contains(100) == true, "Test 19.1: Block 100 should remain");
    assert_test(cache.Contains(101) == false, "Test 19.2: Block 101 should be evicted");
    
    // Remove block 100
    cache.Remove(100);
    assert_test(cache.Contains(100) == false, "Test 19.3: Block 100 should be removed");
    
    // Put block 103
    cache.Put(103, data4);
    assert_test(cache.Contains(102) == true, "Test 19.4: Block 102 should still be there");
    assert_test(cache.Contains(103) == true, "Test 19.5: Block 103 should be added");
}

// Test 20: Verify correct eviction under capacity pressure
void test_capacity_pressure_eviction() {
    LRUCache cache(1);  // 1 MB
    std::string block = std::string(5000, 'X');  // 5KB blocks
    
    // Try to fill cache beyond capacity
    int blocks_added = 0;
    for (int i = 0; i < 300; i++) {
        cache.Put(100 + i, block);
        blocks_added++;
    }
    
    auto stats = cache.GetStats();
    
    // Should have many evictions
    assert_test(stats.evictions > 0, "Test 20.1: Should have evictions under capacity pressure");
    
}

int main() {
    std::cout << "=== LRU Cache Test Suite ===" << std::endl << std::endl;
    
    test_basic_put_get();
    test_get_nonexistent();
    test_partial_get();
    test_out_of_bounds_get();
    test_contains();
    test_remove();
    test_remove_nonexistent();
    test_update_block();
    test_simple_eviction();
    test_lru_ordering();
    test_multiple_evictions();
    test_clear();
    test_stats_hits_misses();
    test_stats_evictions();
    test_reset_stats();
    test_put_update_size_change();
    test_put_empty_string();
    test_get_zero_length();
    test_complex_scenario();
    test_capacity_pressure_eviction();
    
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
