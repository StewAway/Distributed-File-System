#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "fs_service/fs.grpc.pb.h"

// Color codes for output
#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

class FSServerTestClient {
private:
    std::unique_ptr<FSServerService::Stub> server_stub;
    std::string server_addr;
    int test_count = 0;
    int passed_tests = 0;
    int failed_tests = 0;

public:
    FSServerTestClient(const std::string& addr = "localhost:50051") 
        : server_addr(addr) {
        // Create channel to FSServer
        auto channel = grpc::CreateChannel(
            server_addr,
            grpc::InsecureChannelCredentials()
        );
        server_stub = FSServerService::NewStub(channel);
        
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "FSServer gRPC Connection Test" << RESET << std::endl;
        std::cout << BLUE << "Testing: " << server_addr << RESET << std::endl;
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

    // Test 1: Simple HeartBeat to check connection
    // void TestHeartBeat() {
    //     PrintTestHeader("HeartBeat Request");
        
    //     HeartBeatRequest request;
    //     request.set_datanode_id("test-client");
        
    //     HeartBeatResponse response;
    //     grpc::ClientContext context;
        
    //     grpc::Status status = server_stub->HeartBeat(&context, request, &response);
        
    //     if (status.ok()) {
    //         std::cout << "  Response Status: " << response.status() << std::endl;
    //         std::cout << "  Server Available Blocks: " << response.available_blocks() << std::endl;
    //         PrintResult(true, "HeartBeat succeeded");
    //     } else {
    //         std::cout << "  Error Code: " << status.error_code() << std::endl;
    //         std::cout << "  Error Message: " << status.error_message() << std::endl;
    //         PrintResult(false, "HeartBeat failed");
    //     }
    // }

    // Test 2: Write a block
    void TestWriteBlock() {
        PrintTestHeader("WriteBlock Request");
        
        WriteBlockRequest request;
        uint64_t block_uuid = 12345;  // Test block ID
        request.set_block_uuid(block_uuid);
        request.set_data("Hello from test client!");
        request.set_offset(0);
        request.set_sync(true);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        grpc::Status status = server_stub->WriteBlock(&context, request, &response);
        
        if (status.ok()) {
            std::cout << "  Block UUID: " << block_uuid << std::endl;
            std::cout << "  Data Size: " << request.data().size() << " bytes" << std::endl;
            PrintResult(status.ok() && response.success(), 
                        status.ok() ? "WriteBlock response received" : "gRPC error");
        } else {
            std::cout << "  Error Code: " << status.error_code() << std::endl;
            std::cout << "  Error Message: " << status.error_message() << std::endl;
            PrintResult(false, "WriteBlock gRPC call failed");
        }
    }

    // Test 3: Read the block we just wrote
    void TestReadBlock() {
        PrintTestHeader("ReadBlock Request");
        
        ReadBlockRequest request;
        uint64_t block_uuid = 12345;  // Same block from WriteBlock test
        request.set_block_uuid(block_uuid);
        request.set_offset(0);
        request.set_length(0);  // 0 means read entire block
        
        ReadBlockResponse response;
        grpc::ClientContext context;
        
        grpc::Status status = server_stub->ReadBlock(&context, request, &response);
        
        if (status.ok()) {
            std::cout << "  Block UUID: " << block_uuid << std::endl;
            std::cout << "  Success: " << (response.success() ? "true" : "false") << std::endl;
            std::cout << "  Bytes Read: " << response.bytes_read() << std::endl;
            std::cout << "  Data: " << response.data() << std::endl;
            
            if (!response.error().empty()) {
                std::cout << "  Server Error: " << response.error() << std::endl;
            }
            
            PrintResult(response.success(), 
                        response.success() ? "ReadBlock succeeded" : "Block read failed");
        } else {
            std::cout << "  Error Code: " << status.error_code() << std::endl;
            std::cout << "  Error Message: " << status.error_message() << std::endl;
            PrintResult(false, "ReadBlock gRPC call failed");
        }
    }

    // Test 4: Get block info
    void TestGetBlockInfo() {
        PrintTestHeader("GetBlockInfo Request");
        
        GetBlockInfoRequest request;
        uint64_t block_uuid = 12345;  // Same block from WriteBlock test
        request.set_block_uuid(block_uuid);
        
        GetBlockInfoResponse response;
        grpc::ClientContext context;
        
        grpc::Status status = server_stub->GetBlockInfo(&context, request, &response);
        
        if (status.ok()) {
            std::cout << "  Block UUID: " << block_uuid << std::endl;
            std::cout << "  Exists: " << (response.exists() ? "true" : "false") << std::endl;
            std::cout << "  Size: " << response.size() << " bytes" << std::endl;
            std::cout << "  Created At: " << response.created_at() << std::endl;
            std::cout << "  Checksum: " << response.checksum() << std::endl;
            
            PrintResult(status.ok(), "GetBlockInfo request succeeded");
        } else {
            std::cout << "  Error Code: " << status.error_code() << std::endl;
            std::cout << "  Error Message: " << status.error_message() << std::endl;
            PrintResult(false, "GetBlockInfo gRPC call failed");
        }
    }

    // Test 5: Delete block
    void TestDeleteBlock() {
        PrintTestHeader("DeleteBlock Request");
        
        DeleteBlockRequest request;
        uint64_t block_uuid = 12345;  // Delete the test block
        request.set_block_uuid(block_uuid);
        
        StatusResponse response;
        grpc::ClientContext context;
        
        grpc::Status status = server_stub->DeleteBlock(&context, request, &response);
        
        if (status.ok()) {
            std::cout << "  Block UUID: " << block_uuid << std::endl;
            
            PrintResult(status.ok() && response.success(),
                        status.ok() ? "DeleteBlock response received" : "gRPC error");
        } else {
            std::cout << "  Error Code: " << status.error_code() << std::endl;
            std::cout << "  Error Message: " << status.error_message() << std::endl;
            PrintResult(false, "DeleteBlock gRPC call failed");
        }
    }

    // Test 6: Verify block was deleted
    void TestVerifyBlockDeleted() {
        PrintTestHeader("Verify Block Was Deleted");
        
        GetBlockInfoRequest request;
        uint64_t block_uuid = 12345;
        request.set_block_uuid(block_uuid);
        
        GetBlockInfoResponse response;
        grpc::ClientContext context;
        
        grpc::Status status = server_stub->GetBlockInfo(&context, request, &response);
        
        if (status.ok()) {
            std::cout << "  Block UUID: " << block_uuid << std::endl;
            std::cout << "  Block Exists: " << (response.exists() ? "true" : "false") << std::endl;
            
            PrintResult(!response.exists(), 
                        response.exists() ? "Block still exists (deletion failed)" : "Block successfully deleted");
        } else {
            std::cout << "  Error Code: " << status.error_code() << std::endl;
            std::cout << "  Error Message: " << status.error_message() << std::endl;
            PrintResult(false, "GetBlockInfo gRPC call failed");
        }
    }

    void RunAllTests() {
        //TestHeartBeat();
        TestWriteBlock();
        TestReadBlock();
        TestGetBlockInfo();
        TestDeleteBlock();
        TestVerifyBlockDeleted();
        
        PrintSummary();
    }

    void PrintSummary() {
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << BLUE << "Test Summary" << RESET << std::endl;
        std::cout << BLUE << "================================" << RESET << std::endl;
        std::cout << "Total Tests: " << test_count << std::endl;
        std::cout << GREEN << "Passed: " << passed_tests << RESET << std::endl;
        std::cout << RED << "Failed: " << failed_tests << RESET << std::endl;
        
        if (failed_tests == 0 && test_count > 0) {
            std::cout << BLUE << "\n🎉 All tests passed!" << RESET << std::endl;
        } else if (test_count == 0) {
            std::cout << RED << "\n⚠️  No tests were run!" << RESET << std::endl;
        }
        std::cout << std::endl;
    }
};

int main(int argc, char* argv[]) {
    std::string server_address = "localhost:50051";
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--server" && i + 1 < argc) {
            server_address = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: test_grpc_server [options]" << std::endl
                     << "Options:" << std::endl
                     << "  --server <host:port>  Server address (default: localhost:50051)" << std::endl
                     << "  --help                Show this help message" << std::endl;
            return 0;
        }
    }
    
    FSServerTestClient client(server_address);
    client.RunAllTests();
    
    return 0;
}
