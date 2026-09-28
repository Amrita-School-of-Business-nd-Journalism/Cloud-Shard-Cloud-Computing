#ifndef sendFile_H
#define sendFile_H

#include <memory>

#include <grpcpp/grpcpp.h>

#include "shard.grpc.pb.h"

#include <vector>

#include "../localStorage.hpp"
#include "../sqlHelper.hpp"

// using grpc::Server;
// using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using shard::ShardReply;
using shard::ShardRequest;
using shard::Shard;


    // string shard_id_ = 1;
    // string dataset_id_ = 2;
    // uint64 offset = 3;
    // uint64 size = 4;

class GetShardService final : public Shard::Service{
    public:
        Status getShard(ServerContext* connect, const ShardRequest* request, ShardReply* reply) override {
            
            std::vector<std::uint8_t> result;
            
            bool read_success = storage.readShard(
                request->shard_id_(),
                request->dataset_id_(),
                request->offset(),
                request->size(),
                result,
                helper
            );
            if(read_success){
                reply->set_data(
                    reinterpret_cast<const char*>(result.data()),
                    result.size()
                );
                return Status::OK;
            }
            return Status(
                StatusCode::INTERNAL,
                "Failed to read shard from disk"
            );
        }

    private:
        LocalStorage storage;
        sqlHelper helper;
};

#endif