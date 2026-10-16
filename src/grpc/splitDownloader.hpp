#ifndef splitDownloader_H
#define splitDownloader_H

#include <memory>

#include "shard.grpc.pb.h"

#include <vector>

#include "../localStorage.hpp"
#include "../sqlHelper.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using shard::ShardData;
using shard::Shard;

class StoreShardService final : public Shard::Service{
    public:
        Status storeShard(ServerContext* connect, const ShardRequest* request, ShardReply* reply) override {
            std::vector
        }
};

#endif