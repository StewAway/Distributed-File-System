#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "fs_service/fs.grpc.pb.h"

// Color codes for output
#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

class DFSTestClient {
private:
    std::unique_ptr<FSMasterService::Stub> master_stub;
    std::string master_addr = "localhost:50050";
    int test_count = 0;
    int passed_tests = 0;
    int failed_tests = 0;

public:
    DFSTestClient() {
        // Create channel to FSMaster
        auto channel = grpc::CreateChannel(
            master_addr,
            grpc::InsecureChannelCredentials()
        );
        master_stub = FSMasterService::NewStub(channel);
        
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "Distributed File System Test Client" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl << std::endl;
    }

    void PrintTestHeader(const std::string& test_name) {
        test_count++;
        std::cout << YELLOW << "Test " << test_count << ": " << test_name << RESET << std::endl;
    }

    void PrintResult(bool success, const std::string& message = "") {
        if (success) {
            std::cout << GREEN << "✓ PASSED" << RESET;
            passed_tests++;
        } else {
            std::cout << RED << "✗ FAILED" << RESET;
            failed_tests++;
        }
        if (!message.empty()) {
            std::cout << " - " << message;
        }
        std::cout << std::endl << std::endl;
    }

    // ========== Test 1: Mount ==========
    bool TestMount(const std::string& user_id) {
        PrintTestHeader("Mount User to FileSystem");
        
        MountRequest request;
        request.set_user_id(user_id);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Mount(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, user_id);
        return success;
    }

    // ========== Test 2: Open File (Create) ==========
    int TestOpenFile(const std::string& user_id, const std::string& path, 
                      const std::string& mode) {
        PrintTestHeader("Open File");
        std::cout << "  Path: " << path << ", Mode: " << mode << std::endl;
        
        OpenRequest request;
        request.set_user_id(user_id);
        request.set_path(path);
        request.set_mode(mode);
        
        OpenResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Open(&context, request, &response);
        
        bool success = status.ok() && response.fd() > 0;
        PrintResult(success, "fd=" + std::to_string(response.fd()));
        
        return response.fd();
    }

    // ========== Test 3: Write File ==========
    bool TestWrite(const std::string& user_id, int fd, const std::string& data) {
        PrintTestHeader("Write to File");
        std::cout << "  FD: " << fd << ", Data size: " << data.length() << " bytes" << std::endl;
        
        WriteRequest request;
        request.set_user_id(user_id);
        request.set_fd(fd);
        request.set_data(data);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Write(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, std::to_string(data.length()) + " bytes written");
        if (!success) {
            std::cout << "  Error: " << response.error() << std::endl;
        }
        return success;
    }

    // ========== Test 4: Write Large File (Test Block Division) ==========
    bool TestWriteLargeFile(const std::string& user_id, int fd, 
                            uint32_t size_kb) {
        PrintTestHeader("Write Large File (Block Division)");
        std::cout << "  FD: " << fd << ", Size: " << size_kb << " KB" << std::endl;
        
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
        PrintResult(success, std::to_string(size_kb) + " KB written (should divide into blocks)");
        return success;
    }

    // ========== Test 5: Read File ==========
    bool TestRead(const std::string& user_id, int fd, uint32_t count) {
        PrintTestHeader("Read from File");
        std::cout << "  FD: " << fd << ", Count: " << count << " bytes" << std::endl;
        
        ReadRequest request;
        request.set_user_id(user_id);
        request.set_fd(fd);
        request.set_count(count);
        
        ReadResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Read(&context, request, &response);
        
        bool success = status.ok() && response.bytes_read() > 0;
        PrintResult(success, std::to_string(response.bytes_read()) + " bytes read");
        return success;
    }

    // ========== Test 6: Close File ==========
    bool TestClose(const std::string& user_id, const std::string& path) {
        PrintTestHeader("Close File");
        std::cout << "  Path: " << path << std::endl;
        
        CloseRequest request;
        request.set_user_id(user_id);
        request.set_path(path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Close(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, path);
        if (!success) {
            std::cout << "  Error: " << response.error() << std::endl;
        }
        return success;
    }

    // ========== Test 7: Mkdir ==========
    bool TestMkdir(const std::string& user_id, const std::string& dir_path) {
        PrintTestHeader("Create Directory");
        std::cout << "  Path: " << dir_path << std::endl;
        
        MkdirRequest request;
        request.set_user_id(user_id);
        request.set_path(dir_path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Mkdir(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, dir_path);
        return success;
    }

    // ========== Test 8: Ls ==========
    bool TestLs(const std::string& user_id, const std::string& path) {
        PrintTestHeader("List Directory");
        std::cout << "  Path: " << path << std::endl;
        
        LsRequest request;
        request.set_user_id(user_id);
        request.set_path(path);
        
        LsResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Ls(&context, request, &response);
        
        bool success = status.ok();
        std::string msg = std::to_string(response.files_size()) + " items";
        PrintResult(success, msg);
        
        if (success && response.files_size() > 0) {
            std::cout << "  Contents:" << std::endl;
            for (int i = 0; i < response.files_size(); i++) {
                std::cout << "    - " << response.files(i) << std::endl;
            }
            std::cout << std::endl;
        }
        
        return success;
    }

    // ========== Test 9: Rmdir ==========
    bool TestRmdir(const std::string& user_id, const std::string& dir_path) {
        PrintTestHeader("Remove Directory");
        std::cout << "  Path: " << dir_path << std::endl;
        
        RmdirRequest request;
        request.set_user_id(user_id);
        request.set_path(dir_path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->Rmdir(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, dir_path);
        return success;
    }

    // ========== Test 10: DeleteFile ==========
    bool TestDeleteFile(const std::string& user_id, const std::string& file_path) {
        PrintTestHeader("Delete File");
        std::cout << "  Path: " << file_path << std::endl;
        
        DeleteFileRequest request;
        request.set_user_id(user_id);
        request.set_path(file_path);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->DeleteFile(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, file_path);
        return success;
    }

    // ========== Test 11: Unmount ==========
    bool TestUnmount(const std::string& user_id) {
        PrintTestHeader("Unmount User");
        
        MountRequest request;
        request.set_user_id(user_id);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        auto status = master_stub->UnMount(&context, request, &response);
        
        bool success = status.ok() && response.success();
        PrintResult(success, user_id);
        return success;
    }

    // ========== Test Summary ==========
    void PrintSummary() {
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "Test Summary" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << "Total Tests: " << test_count << std::endl;
        std::cout << GREEN << "Passed: " << passed_tests << RESET << std::endl;
        std::cout << RED << "Failed: " << failed_tests << RESET << std::endl;
        std::cout << "Pass Rate: " << (test_count > 0 ? (passed_tests * 100 / test_count) : 0) 
                  << "%" << std::endl << std::endl;
    }

    int GetFailedTests() const {
        return failed_tests;
    }
};

int main(int argc, char* argv[]) {
    DFSTestClient client;
    
    std::string user_id = "test_user_001";
    
    std::cout << "Note: Make sure fs_master is running on localhost:50050" << std::endl;
    std::cout << "Note: Make sure at least one fs_server is running" << std::endl << std::endl;
    
    // Give servers time to start (if running in same process)
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // ========== Test Sequence ==========
    
    // 1. Mount user
    client.TestMount(user_id);
    
    // 2. Create and write file
    int fd1 = client.TestOpenFile(user_id, "/testfile.txt", "w");
    if (fd1 > 0) {
        client.TestWrite(user_id, fd1, "Hello, Distributed File System!");
        client.TestClose(user_id, "/testfile.txt");
    }
    
    // 3. Write large file (test block division - 200 KB)
    int fd2 = client.TestOpenFile(user_id, "/largefile.bin", "w");
    if (fd2 > 0) {
        client.TestWriteLargeFile(user_id, fd2, 200);  // 200 KB
        client.TestClose(user_id, "/largefile.bin");
    }
    
    // 4. Create directory
    client.TestMkdir(user_id, "/mydir");
    
    // 5. List root directory
    client.TestLs(user_id, "/");
    
    // 6. Remove directory
    client.TestRmdir(user_id, "/mydir");
    
    // 7. Delete file
    client.TestDeleteFile(user_id, "/testfile.txt");
    
    // 8. Unmount user
    client.TestUnmount(user_id);
    
    // Print summary
    client.PrintSummary();
    
    return client.GetFailedTests() == 0 ? 0 : 1;
}
