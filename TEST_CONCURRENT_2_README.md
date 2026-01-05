# test_concurrent_2.cpp - Concurrency Correctness Test

## Overview
`test_concurrent_2.cpp` is a comprehensive concurrency test that validates your DFS implementation under concurrent access patterns. It spawns **2 concurrent threads**, each making **complex gRPC calls** to the file system master and performing realistic file operations.

## What It Tests

### Concurrency Scenarios
- **Simultaneous User Mounting**: Two different users mount simultaneously
- **Concurrent File Operations**: Both threads create, write, read, and delete files
- **Block Division Under Load**: Large file writes (150 KB each) are divided into blocks while another thread operates
- **Directory Operations**: Concurrent mkdir, ls, and rmdir calls
- **Resource Contention**: Tests if the file descriptor allocation, inode table, and block allocation handle concurrent access correctly

### Test Operations Per Thread
1. **Mount** - User mounts to the filesystem
2. **Create and Write File** - Creates a file and writes data to it
3. **Write Large File** - Writes 150 KB file (triggers block division)
4. **Create Directory** - Creates a directory
5. **Read File** - Opens and reads the previously created file
6. **List Directory** - Lists root directory contents
7. **Remove Directory** - Removes the created directory
8. **Delete Files** - Deletes created files
9. **Unmount** - User unmounts from filesystem

## How to Run

### Prerequisites
Make sure you have the following running:
```bash
# Terminal 1: Start the master
./build/fs_master

# Terminal 2: Start the file server(s)
./build/fs_server
```

### Execute the Test
```bash
./build/test_concurrent_2
```

### Expected Output
The test will display:
- Color-coded output showing which thread is executing which operation
- Real-time pass/fail status for each operation
- Summary at the end with total tests run, passed, and failed
- Details of any failures

Example output:
```
[Thread 0] ✓ PASSED - concurrent_user_0
[Thread 1] ✓ PASSED - concurrent_user_1
[Thread 0] ✓ PASSED - Path: /file1_thread0.txt, FD: 3
[Thread 1] ✓ PASSED - Path: /file1_thread1.txt, FD: 4
...
================================
Test Summary
================================
Total Tests: 40
Passed: 40
Failed: 0
Pass Rate: 100%
```

## Key Features

### Thread-Safe Result Tracking
- Uses mutex-protected shared state to track test results
- No race conditions in test result logging

### Detailed Failure Reporting
- Records which thread caused any failures
- Shows exactly which operation failed with context

### Realistic File Operations
- Replicates the same file operations used in `test_mount_client.cpp`
- Tests block division with 150 KB files (larger than typical block size)
- Exercises the entire gRPC API surface

### Performance Characteristics
- Each thread performs 10 major operations (Mount, Open, Write, Read, Close, Mkdir, Ls, Rmdir, DeleteFile, Unmount)
- Total of ~20-30 individual gRPC calls depending on operation complexity
- Includes small delays between operations to allow time for internal processing

## Concurrency Issues It Can Detect

✓ **Race Conditions in Inode Table** - Concurrent file creations with shared inode allocation  
✓ **Block Allocation Conflicts** - Multiple threads allocating blocks simultaneously  
✓ **User Context Corruption** - FD table or user metadata shared state issues  
✓ **File Descriptor Leaks** - Multiple threads opening/closing files  
✓ **Directory Entry Inconsistencies** - Concurrent mkdir/rmdir/ls operations  
✓ **Lock Deadlocks** - If your locking mechanism causes deadlocks  
✓ **Data Corruption** - If concurrent writes/reads cause data loss  

## Implementation Details

### Thread Architecture
```cpp
// Each thread:
// 1. Creates its own gRPC stub/channel (independent connections)
// 2. Has a unique user_id (prevents user-level conflicts)
// 3. Uses thread-safe result tracking (mutex-protected)
// 4. Reports all operations with thread identification
```

### Results Tracking
- `TestResults` struct holds shared state with mutex protection
- Each test call increments either `passed_tests` or `failed_tests`
- Failures are logged with thread ID and operation details

## Next Steps

### If Tests Pass
🎉 Your concurrency implementation is solid! Consider:
- Running multiple times to catch intermittent issues
- Increasing to 4+ threads for stress testing
- Running with large file sizes to push block allocation

### If Tests Fail
1. Check the failure details - which operation failed?
2. Look at the thread IDs involved
3. Add logging to your fs_master implementation to trace the issue
4. Consider using thread sanitizers (TSan) for additional debugging
5. Review your mutex usage and lock ordering

## Running with Additional Tools

### With ThreadSanitizer
```bash
# Rebuild with TSan enabled, then:
./build/test_concurrent_2
```

### With GDB
```bash
gdb ./build/test_concurrent_2
(gdb) run
```

## Modification Ideas

You can extend this test by:
- Increasing NUM_THREADS from 2 to 4, 8, etc.
- Adding more operations per thread
- Creating shared files (both threads write to same file)
- Testing error conditions (operations on non-existent files)
- Measuring performance/latency
