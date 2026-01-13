# Multi-stage build for the Distributed File System
FROM ubuntu:22.04 AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libgrpc-dev \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libssl-dev \
    uuid-dev \
    pkg-config \
    libabsl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the entire project
COPY . .

# Create cmake config file for gRPC (Ubuntu 22.04 doesn't ship with it)
RUN mkdir -p /usr/share/cmake/grpc && echo '\
set(gRPC_FOUND TRUE)\n\
set(gRPC_INCLUDE_DIRS "/usr/include")\n\
set(gRPC_LIBRARIES grpc++ grpc gpr)\n\
if(NOT TARGET gRPC::grpc++)\n\
  add_library(gRPC::grpc++ INTERFACE IMPORTED)\n\
  set_target_properties(gRPC::grpc++ PROPERTIES\n\
    INTERFACE_INCLUDE_DIRECTORIES "/usr/include"\n\
    INTERFACE_LINK_LIBRARIES "grpc++;grpc;gpr;absl_synchronization;absl_base;absl_raw_logging_internal;protobuf")\n\
endif()\n\
' > /usr/share/cmake/grpc/gRPCConfig.cmake

# Regenerate protobuf/gRPC files to match the installed gRPC version
RUN cd /build/common/protos/fs_service && \
    protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) fs.proto

# Create and enter build directory
RUN mkdir -p /build/build && cd /build/build && \
    cmake -DCMAKE_PREFIX_PATH="/usr/share/cmake" .. && \
    make -j$(nproc)

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libgrpc++1 \
    libprotobuf23 \
    libssl3 \
    uuid-runtime \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy built binaries from builder
COPY --from=builder /build/build/fs_master /app/fs_master
COPY --from=builder /build/build/fs_server /app/fs_server

# Create directories for data nodes
RUN mkdir -p /data/datanode1 /data/datanode2 /data/datanode3

# Default command will be overridden by docker-compose
CMD ["/bin/bash"]
