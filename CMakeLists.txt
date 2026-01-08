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
    fs_master/src/user_context.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(fs_master ${FSMASTER_SOURCES})
target_link_libraries(fs_master gRPC::grpc++ protobuf::libprotobuf uuid)

# Source files for fs_server (datanode)
set(FSSERVER_SOURCES
    fs_server/src/main.cpp
    fs_server/src/fsserver_service.cpp
    fs_server/src/block_store.cpp
    fs_server/src/disk.cpp
    fs_server/src/cache.cpp
    fs_server/src/lru_cache.cpp
    fs_server/src/lfu_cache.cpp
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

# # Test gRPC server connection directly
# set(TEST_GRPC_SERVER_SOURCES
#     testing/test_grpc_server.cpp
#     common/protos/fs_service/fs.grpc.pb.cc
#     common/protos/fs_service/fs.pb.cc
# )

# add_executable(test_grpc_server ${TEST_GRPC_SERVER_SOURCES})
# target_link_libraries(test_grpc_server gRPC::grpc++ protobuf::libprotobuf)

# # Test concurrent allocation
# set(TEST_CONCURRENT_ALLOCATION_SOURCES
#     testing/test_concurrent_allocation.cpp
#     fs_master/src/user_context.cpp
#     fs_master/src/inode.cpp
# )

# add_executable(test_concurrent_allocation ${TEST_CONCURRENT_ALLOCATION_SOURCES})
# target_link_libraries(test_concurrent_allocation)

# # Test concurrent inode table access
# set(TEST_CONCURRENT_INODE_TABLE_SOURCES
#     testing/test_concurrent_inode_table.cpp
#     fs_master/src/user_context.cpp
#     fs_master/src/inode.cpp
# )

# add_executable(test_concurrent_inode_table ${TEST_CONCURRENT_INODE_TABLE_SOURCES})
# target_link_libraries(test_concurrent_inode_table)

# # Test FSMaster integration with concurrent operations
# set(TEST_FSMASTER_INTEGRATION_SOURCES
#     testing/test_fsmaster_integration.cpp
#     fs_master/src/fsmaster_service.cpp
#     fs_master/src/user_context.cpp
#     fs_master/src/inode.cpp
#     common/protos/fs_service/fs.grpc.pb.cc
#     common/protos/fs_service/fs.pb.cc
# )

# add_executable(test_fsmaster_integration ${TEST_FSMASTER_INTEGRATION_SOURCES})
# target_link_libraries(test_fsmaster_integration gRPC::grpc++ protobuf::libprotobuf uuid)

# # Test concurrent user operations
# set(TEST_CONCURRENT_USERS_SOURCES
#     testing/test_concurrent_users.cpp
#     fs_master/src/user_context.cpp
#     fs_master/src/inode.cpp
# )

# add_executable(test_concurrent_users ${TEST_CONCURRENT_USERS_SOURCES})
# target_link_libraries(test_concurrent_users)

# Test concurrent gRPC calls (2 threads with complex operations)
set(TEST_CONCURRENT_2_SOURCES
    testing/test_concurrent_2.cpp
    common/protos/fs_service/fs.grpc.pb.cc
    common/protos/fs_service/fs.pb.cc
)

add_executable(test_concurrent_2 ${TEST_CONCURRENT_2_SOURCES})
target_link_libraries(test_concurrent_2 gRPC::grpc++ protobuf::libprotobuf)

# Test LRU Cache correctness
set(TEST_LRU_CACHE_SOURCES
    testing/test_lru_cache.cpp
    fs_server/src/lru_cache.cpp
)

add_executable(test_lru_cache ${TEST_LRU_CACHE_SOURCES})
target_link_libraries(test_lru_cache)

# # Benchmark: BlockStore performance (Tier 2 - measure access patterns)
# set(BENCHMARK_BLOCK_STORE_SOURCES
#     testing/benchmark_block_store.cpp
# )

# add_executable(benchmark_block_store ${BENCHMARK_BLOCK_STORE_SOURCES})
# target_link_libraries(benchmark_block_store)