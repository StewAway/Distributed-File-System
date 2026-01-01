cmake_minimum_required(VERSION 3.10)
project(DistributedFileSystem)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/common/protos)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/fs_master/include)

# Source files
set(SOURCES
    fs_master/src/main.cpp
    fs_master/src/fsmaster_service.cpp
    fs_master/src/inode.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(fs_master ${SOURCES})
target_link_libraries(fs_master gRPC::grpc++ protobuf::libprotobuf uuid)

# Test client
set(TEST_CLIENT_SOURCES
    testing/test_mount_client.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(test_mount_client ${TEST_CLIENT_SOURCES})
target_link_libraries(test_mount_client gRPC::grpc++ protobuf::libprotobuf)