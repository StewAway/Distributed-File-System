cmake_minimum_required(VERSION 3.10)
project(DistributedFileSystem)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/common/protos)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/fs_master/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/fs_server/include)

# Source files for fs_master
set(FSMASTER_SOURCES
    fs_master/src/main.cpp
    fs_master/src/fsmaster_service.cpp
    fs_master/src/inode.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(fs_master ${FSMASTER_SOURCES})
target_link_libraries(fs_master gRPC::grpc++ protobuf::libprotobuf uuid)

# Source files for fs_server (datanode)
set(FSSERVER_SOURCES
    fs_server/src/main.cpp
    fs_server/src/fsserver_service.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(fs_server ${FSSERVER_SOURCES})
target_link_libraries(fs_server gRPC::grpc++ protobuf::libprotobuf OpenSSL::Crypto)

# Test client
set(TEST_CLIENT_SOURCES
    testing/test_mount_client.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(test_mount_client ${TEST_CLIENT_SOURCES})
target_link_libraries(test_mount_client gRPC::grpc++ protobuf::libprotobuf)

# Test gRPC server connection directly
set(TEST_GRPC_SERVER_SOURCES
    testing/test_grpc_server.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(test_grpc_server ${TEST_GRPC_SERVER_SOURCES})
target_link_libraries(test_grpc_server gRPC::grpc++ protobuf::libprotobuf)