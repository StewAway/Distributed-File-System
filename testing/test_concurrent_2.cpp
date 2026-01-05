#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>
#include <grpcpp/grpcpp.h>
#include "fs_service/fs.grpc.pb.h"

// Color codes for output
#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define CYAN "\033[36m"
#define RESET "\033[0m"

// Global state for tracking test results
struct TestResults {
    std::mutex lock;
    int total_tests = 0;
    int passed_tests = 0;
    int failed_tests = 0;
    std::vector<std::string> failures;

    void AddPass() {
        std::lock_guard<std::mutex> guard(lock);
        passed_tests++;
        total_tests++;
    }

    void AddFail(const std::string& msg) {
        std::lock_guard<std::mutex> guard(lock);
        failed_tests++;
        total_tests++;
        failures.push_back(msg);
    }

    void PrintSummary() {
        std::lock_guard<std::mutex> guard(lock);
        std::cout << BLUE << "\n================================" << RESET << std::endl;
        std::cout << BLUE << "Test Summary" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << "Total Tests: " << total_tests << std::endl;
        std::cout << GREEN << "Passed: " << passed_tests << RESET << std::endl;
        std::cout << RED << "Failed: " << failed_tests << RESET << std::endl;
        if (total_tests > 0) {
            std::cout << "Pass Rate: " << (passed_tests * 100 / total_tests) << "%" << std::endl;
        }
        if (!failures.empty()) {
            std::cout << RED << "\nFailures:" << RESET << std::endl;
            for (const auto& failure : failures) {
                std::cout << "  - " << failure << std::endl;
            }
        }
        std::cout << std::endl;
    }
};

class ConcurrentDFSTestClient {
private:
    std::unique_ptr<FSMasterService::Stub> master_stub;
    std::string master_addr = "localhost:50050";
    TestResults& results;
    int thread_id;

public:
    ConcurrentDFSTestClient(int id, TestResults& res) 
        : results(res), thread_id(id) {
        // Create channel to FSMaster
        auto channel = grpc::CreateChannel(
            master_addr,
            grpc::InsecureChannelCredentials()
        );
        master_stub = FSMasterService::NewStub(channel);
    }

    void PrintThreadMessage(const std::string& msg) {
        std::cout << CYAN << "[Thread " << thread_id << "] " << RESET << msg << std::endl;
    }

    void PrintTestStart(const std::string& test_name) {
        PrintThreadMessage(std::string(YELLOW) + test_name + RESET);
    }

    void PrintTestResult(bool success, const std::string& details = "") {
        if (success) {
            PrintThreadMessage(std::string(GREEN) + "✓ PASSED" + RESET + " " + details);
            results.AddPass();
        } else {
            PrintThreadMessage(std::string(RED) + "✗ FAILED" + RESET + " " + details);
            results.AddFail("[Thread " + std::to_string(thread_id) + "] " + details);
        }
    }

    // ========== Test: Mount ==========
    bool TestMount(const std::string& user_id) {
        PrintTestStart("Mount User");
        
        MountRequest request;
        request.set_user_id(user_id);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Mount(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, user_id);
        return success;
    }

    // ========== Test: Open File ==========
    int TestOpenFile(const std::string& user_id, const std::string& path, 
                     const std::string& mode) {
        PrintTestStart("Open File: " + path);
        
        OpenRequest request;
        request.set_user_id(user_id);
        request.set_path(path);
        request.set_mode(mode);
        
        OpenResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Open(&context, request, &response);
        
        bool success = status.ok() && response.fd() > 0;
        PrintTestResult(success, "Path: " + path + ", FD: " + std::to_string(response.fd()));
        
        return response.fd();
    }

    // ========== Test: Write File ==========
    bool TestWrite(const std::string& user_id, int fd, const std::string& data) {
        PrintTestStart("Write to File");
        
        WriteRequest request;
        request.set_user_id(user_id);
        request.set_fd(fd);
        request.set_data(data);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Write(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, "FD: " + std::to_string(fd) + ", Size: " + std::to_string(data.length()) + " bytes");
        if (!success) {
            PrintThreadMessage("Error: " + response.error());
        }
        return success;
    }

    // ========== Test: Write Large File (Block Division) ==========
    bool TestWriteLargeFile(const std::string& user_id, int fd, uint32_t size_kb) {
        PrintTestStart("Write Large File (Block Division)");
        
        // Generate data
        std::string data;
        data.reserve(size_kb * 1024);
        for (uint32_t i = 0; i < size_kb * 1024; i++) {
            data += (char)('A' + (i % 26));
        }
        
        WriteRequest request;
        request.set_user_id(user_id);
        request.set_fd(fd);
        request.set_data(data);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Write(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, std::to_string(size_kb) + " KB written (should divide into blocks)");
        return success;
    }

    // ========== Test: Read File ==========
    bool TestRead(const std::string& user_id, int fd, uint32_t count) {
        PrintTestStart("Read from File");
        
        ReadRequest request;
        request.set_user_id(user_id);
        request.set_fd(fd);
        request.set_count(count);
        
        ReadResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Read(&context, request, &response);
        
        bool success = status.ok() && response.bytes_read() > 0;
        PrintTestResult(success, "FD: " + std::to_string(fd) + ", Bytes read: " + std::to_string(response.bytes_read()));
        return success;
    }

    // ========== Test: Close File ==========
    bool TestClose(const std::string& user_id, int fd) {
        PrintTestStart("Close File");
        
        CloseRequest request;
        request.set_user_id(user_id);
        request.set_fd(fd);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Close(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, "FD: " + std::to_string(fd));
        if (!success) {
            PrintThreadMessage("Error: " + response.error());
        }
        return success;
    }

    // ========== Test: Mkdir ==========
    bool TestMkdir(const std::string& user_id, const std::string& dir_path) {
        PrintTestStart("Create Directory");
        
        MkdirRequest request;
        request.set_user_id(user_id);
        request.set_path(dir_path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Mkdir(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, "Path: " + dir_path);
        return success;
    }

    // ========== Test: Ls ==========
    bool TestLs(const std::string& user_id, const std::string& path) {
        PrintTestStart("List Directory");
        
        LsRequest request;
        request.set_user_id(user_id);
        request.set_path(path);
        
        LsResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Ls(&context, request, &response);
        
        bool success = status.ok();
        PrintTestResult(success, "Path: " + path + ", Items: " + std::to_string(response.files_size()));
        
        if (success && response.files_size() > 0) {
            PrintThreadMessage("Directory contents:");
            for (int i = 0; i < response.files_size(); i++) {
                PrintThreadMessage("  - " + response.files(i));
            }
        }
        
        return success;
    }

    // ========== Test: Rmdir ==========
    bool TestRmdir(const std::string& user_id, const std::string& dir_path) {
        PrintTestStart("Remove Directory");
        
        RmdirRequest request;
        request.set_user_id(user_id);
        request.set_path(dir_path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Rmdir(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, "Path: " + dir_path);
        if (!success) {
            PrintThreadMessage("Error: " + response.error());
        }
        return success;
    }

    // ========== Test: DeleteFile ==========
    bool TestDeleteFile(const std::string& user_id, const std::string& file_path) {
        PrintTestStart("Delete File");
        
        DeleteFileRequest request;
        request.set_user_id(user_id);
        request.set_path(file_path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->DeleteFile(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, "Path: " + file_path);
        if (!success) {
            PrintThreadMessage("Error: " + response.error());
        }
        return success;
    }

    // ========== Test: Unmount ==========
    bool TestUnmount(const std::string& user_id) {
        PrintTestStart("Unmount User");
        
        MountRequest request;
        request.set_user_id(user_id);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->UnMount(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintTestResult(success, user_id);
        return success;
    }

    // ========== Complex Test Sequence ==========
    void RunComplexTestSequence(const std::string& user_id) {
        PrintThreadMessage(std::string(BLUE) + "=== Starting Complex Test Sequence ===" + RESET);
        
        // 1. Mount user
        if (!TestMount(user_id)) {
            PrintThreadMessage(std::string(RED) + "Failed to mount, aborting" + RESET);
            return;
        }
        
        // Sleep to simulate work
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 2. Create and write multiple files
        int fd1 = TestOpenFile(user_id, "/file1_thread" + std::to_string(thread_id) + ".txt", "w");
        if (fd1 > 0) {
            TestWrite(user_id, fd1, "Hello from thread " + std::to_string(thread_id) + "!");
            TestClose(user_id, fd1);
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 3. Create and write large file (test block division - 150 KB)
        int fd2 = TestOpenFile(user_id, "/largefile_thread" + std::to_string(thread_id) + ".bin", "w");
        if (fd2 > 0) {
            TestWriteLargeFile(user_id, fd2, 150);  // 150 KB
            TestClose(user_id, fd2);
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 4. Create directory
        std::string dir_path = "/thread" + std::to_string(thread_id) + "_dir";
        TestMkdir(user_id, dir_path);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 5. Open file for reading (the one just created)
        int fd3 = TestOpenFile(user_id, "/file1_thread" + std::to_string(thread_id) + ".txt", "r");
        if (fd3 > 0) {
            TestRead(user_id, fd3, 100);
            TestClose(user_id, fd3);
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 6. List root directory
        TestLs(user_id, "/");
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 7. Remove directory
        TestRmdir(user_id, dir_path);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 8. Delete first file
        TestDeleteFile(user_id, "/file1_thread" + std::to_string(thread_id) + ".txt");
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 9. Delete large file
        TestDeleteFile(user_id, "/largefile_thread" + std::to_string(thread_id) + ".bin");
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // 10. Unmount user
        TestUnmount(user_id);
        
        PrintThreadMessage(std::string(BLUE) + "=== Complex Test Sequence Complete ===" + RESET);
    }
};

// Thread worker function
void ThreadWorker(int thread_id, TestResults& results) {
    ConcurrentDFSTestClient client(thread_id, results);
    std::string user_id = "concurrent_user_" + std::to_string(thread_id);
    
    // Run complex test sequence for this thread
    client.RunComplexTestSequence(user_id);
}

int main(int argc, char* argv[]) {
    std::cout << BLUE << "================================" << RESET << std::endl;
    std::cout << BLUE << "Concurrent DFS Test (2 Threads)" << RESET << std::endl;
    std::cout << BLUE << "================================" << RESET << std::endl << std::endl;
    
    std::cout << "Note: Make sure fs_master is running on localhost:50050" << std::endl;
    std::cout << "Note: Make sure at least one fs_server is running" << std::endl << std::endl;
    
    // Give servers time to start
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    TestResults results;
    
    // Create and launch 2 threads
    std::vector<std::thread> threads;
    const int NUM_THREADS = 2;
    
    std::cout << CYAN << "Launching " << NUM_THREADS << " concurrent threads..." << RESET << std::endl << std::endl;
    
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back(ThreadWorker, i, std::ref(results));
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Print summary
    results.PrintSummary();
    
    return results.failed_tests == 0 ? 0 : 1;
}
