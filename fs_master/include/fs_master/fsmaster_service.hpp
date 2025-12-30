#pragma once
#include "fs.grpc.pb.h"

namespace fs_master {

class FSMasterServiceImpl final : public FSMasterService::Service {
public:
    grpc::Status Mount(grpc::ServerContext*, const MountRequest*, StatusResponse*) override;
    grpc::Status Open(grpc::ServerContext*, const OpenRequest*, OpenResponse*) override;
    grpc::Status Read(grpc::ServerContext*, const ReadRequest*, ReadResponse*) override;
    grpc::Status Write(grpc::ServerContext*, const WriteRequest*, StatusResponse*) override;
    grpc::Status Close(grpc::ServerContext*, const CloseRequest*, StatusResponse*) override;
    grpc::Status Mkdir(grpc::ServerContext*, const MkdirRequest*, StatusResponse*) override;
    grpc::Status Rmdir(grpc::ServerContext*, const RmdirRequest*, StatusResponse*) override;
    grpc::Status Ls(grpc::ServerContext*, const LsRequest*, LsResponse*) override;
    grpc::Status DeleteFile(grpc::ServerContext*, const DeleteFileRequest*, StatusResponse*) override;
};

}