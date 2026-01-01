#include <iostream>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "fs_service/fs.grpc.pb.h"

int main(int argc, char* argv[]) {
    // Create a channel to connect to the server
    std::string target_str = "localhost:50050";
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        target_str,
        grpc::InsecureChannelCredentials()
    );

    // Create a stub (client)
    std::unique_ptr<FSMasterService::Stub> stub = 
        FSMasterService::NewStub(channel);

    // Prepare the Mount request
    MountRequest request;
    request.set_user_id("test_user_001");

    // Prepare the response
    StatusResponse response;

    // Create a context for the RPC
    grpc::ClientContext context;

    // Make the RPC call
    std::cout << "Calling Mount RPC with user_id: " << request.user_id() << std::endl;
    grpc::Status status = stub->Mount(&context, request, &response);

    // Check the status
    if (status.ok()) {
        std::cout << "\n✓ RPC succeeded!" << std::endl;
        std::cout << "Response:" << std::endl;
        std::cout << "  success: " << response.success() << std::endl;
        std::cout << "  error: " << response.error() << std::endl;
        return 0;
    } else {
        std::cout << "\n✗ RPC failed!" << std::endl;
        std::cout << "Error code: " << status.error_code() << std::endl;
        std::cout << "Error message: " << status.error_message() << std::endl;
        return 1;
    }
}
