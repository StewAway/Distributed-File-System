#include <iostream>
#include <thread>
#include <vector>
#include <set>
#include <atomic>
#include <chrono>
#include <cassert>
#include "../fs_master/include/fs_master/user_context.hpp"

using namespace std;

// Global counters for verification
std::atomic<int> mount_count(0);
std::atomic<int> unmount_count(0);
std::atomic<int> successful_operations(0);
std::atomic<int> failed_operations(0);

void concurrent_mount_unmount(int user_num, int iterations) {
    string user_id = "user_" + to_string(user_num);
    
    for (int i = 0; i < iterations; i++) {
        // Mount user
        fs_master::PutUserContext(user_id, fs_master::UserContext());
        fs_master::SetUserRoot(user_id, user_num * 100 + i);
        mount_count++;
        
        // Verify user exists
        if (fs_master::UserExists(user_id)) {
            successful_operations++;
        } else {
            failed_operations++;
            cout << "ERROR: User " << user_id << " should exist after mount!\n";
        }
        
        // Verify user root
        auto root_opt = fs_master::GetUserRoot(user_id);
        if (root_opt.has_value() && root_opt.value() == (user_num * 100 + i)) {
            successful_operations++;
        } else {
            failed_operations++;
            cout << "ERROR: User " << user_id << " root mismatch!\n";
        }
        
        // Unmount user
        fs_master::RemoveUser(user_id);
        unmount_count++;
        
        // Verify user is gone
        if (!fs_master::UserExists(user_id)) {
            successful_operations++;
        } else {
            failed_operations++;
            cout << "ERROR: User " << user_id << " should not exist after unmount!\n";
        }
    }
}

void concurrent_user_operations(int user_num, int operations) {
    string user_id = "user_" + to_string(user_num);
    
    // Mount the user
    fs_master::PutUserContext(user_id, fs_master::UserContext());
    fs_master::SetUserRoot(user_id, user_num * 1000);
    
    for (int i = 0; i < operations; i++) {
        // Get user context
        auto ctx_opt = fs_master::GetUserContext(user_id);
        if (!ctx_opt.has_value()) {
            failed_operations++;
            continue;
        }
        
        // Modify context (simulate adding a file descriptor)
        auto ctx = ctx_opt.value();
        fs_master::FileSession session;
        session.inode_id = user_num * 1000 + i;
        session.offset = 0;
        session.mode = "r";
        ctx.open_files[i] = session;
        
        // Put modified context back
        fs_master::PutUserContext(user_id, ctx);
        successful_operations++;
        
        // Verify GetUserContextAndRoot works
        auto combined = fs_master::GetUserContextAndRoot(user_id);
        if (combined.has_value()) {
            successful_operations++;
        } else {
            failed_operations++;
        }
    }
    
    // Unmount
    fs_master::RemoveUser(user_id);
}

int main() {
    cout << "========================================\n";
    cout << "Testing Concurrent User Operations\n";
    cout << "========================================\n\n";
    
    // Test 1: Concurrent Mount/Unmount
    cout << "Test 1: Concurrent Mount/Unmount\n";
    cout << "  Spawning 20 threads, each mounting/unmounting 50 times...\n";
    {
        vector<thread> threads;
        mount_count = 0;
        unmount_count = 0;
        successful_operations = 0;
        failed_operations = 0;
        
        auto start = chrono::high_resolution_clock::now();
        
        for (int i = 0; i < 20; i++) {
            threads.emplace_back(concurrent_mount_unmount, i, 50);
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cout << "  ✓ Total mounts: " << mount_count << "\n";
        cout << "  ✓ Total unmounts: " << unmount_count << "\n";
        cout << "  ✓ Successful operations: " << successful_operations << "\n";
        cout << "  ✓ Failed operations: " << failed_operations << "\n";
        cout << "  ✓ Time: " << duration.count() << "ms\n";
        
        if (mount_count == 1000 && unmount_count == 1000 && failed_operations == 0) {
            cout << "  ✓ PASS: All mount/unmount operations successful\n\n";
        } else {
            cout << "  ✗ FAIL: Some operations failed\n\n";
            return 1;
        }
    }
    
    // Test 2: Concurrent User Operations (multiple users simultaneously)
    cout << "Test 2: Concurrent User Operations\n";
    cout << "  Spawning 10 threads (users), each performing 100 operations...\n";
    {
        vector<thread> threads;
        successful_operations = 0;
        failed_operations = 0;
        
        auto start = chrono::high_resolution_clock::now();
        
        for (int i = 0; i < 10; i++) {
            threads.emplace_back(concurrent_user_operations, i, 100);
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cout << "  ✓ Successful operations: " << successful_operations << "\n";
        cout << "  ✓ Failed operations: " << failed_operations << "\n";
        cout << "  ✓ Time: " << duration.count() << "ms\n";
        
        // Each user does 100 operations, gets context 100 times, puts context 100 times,
        // and GetUserContextAndRoot 100 times = 300 ops per user * 10 users = 3000 expected
        if (failed_operations == 0) {
            cout << "  ✓ PASS: All concurrent user operations successful\n\n";
        } else {
            cout << "  ✗ FAIL: Some operations failed\n\n";
            return 1;
        }
    }
    
    // Test 3: Verify no users remain after all operations
    cout << "Test 3: Cleanup Verification\n";
    {
        bool users_exist = false;
        for (int i = 0; i < 20; i++) {
            string user_id = "user_" + to_string(i);
            if (fs_master::UserExists(user_id)) {
                users_exist = true;
                cout << "  ✗ ERROR: User " << user_id << " still exists after unmount!\n";
            }
        }
        
        if (!users_exist) {
            cout << "  ✓ PASS: All users successfully cleaned up\n\n";
        } else {
            cout << "  ✗ FAIL: Some users not cleaned up\n\n";
            return 1;
        }
    }
    
    cout << "========================================\n";
    cout << "All tests passed!\n";
    cout << "========================================\n";
    
    return 0;
}
