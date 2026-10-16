#ifndef sendFile_H
#define sendFile_H

#include <memory>
#include <grpcpp/grpcpp.h>
#include "shard.grpc.pb.h"
#include <vector>

#include "../localStorage.hpp"
#include "../sqlHelper.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using shard::ShardReply;
using shard::ShardRequest;
using shard::Shard;
using shard::ListShardsRequest;
using shard::ListShardsReply;

class GetShardService final : public Shard::Service {
public:
    Status getShard(
        ServerContext* context,
        const ShardRequest* request,
        ShardReply* reply
    ) override {

        std::vector<std::uint8_t> result;

        bool read_success = storage.readShard(
            request->shard_id_(),
            request->dataset_id_(),
            request->offset(),
            request->size(),
            result,
            helper
        );

        if (read_success) {
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

    Status listShards(
        ServerContext* context,
        const ListShardsRequest* request,
        ListShardsReply* reply
    ) override {

        const std::string dataset_id = request->dataset_id_();

        std::vector<std::string> shard_ids =
            storage.listShards(dataset_id, helper);

        for (const auto& shard_id : shard_ids) {
            std::vector<std::uint8_t> data;

            // Read the complete shard from disk.
            // The size is obtained from the file itself.
            if (!storage.readShard(
                    shard_id,
                    dataset_id,
                    0,
                    std::filesystem::file_size(
                        helper.getLocation(shard_id, dataset_id)
                    ),
                    data,
                    helper
                )) {
                return Status(
                    StatusCode::INTERNAL,
                    "Failed to read shard: " + shard_id
                );
            }

            auto* shard_info = reply->add_shards();

            shard_info->set_shard_id_(shard_id);
            shard_info->set_size_(data.size());

            if (!data.empty()) {
                shard_info->set_data_(
                    reinterpret_cast<const char*>(data.data()),
                    data.size()
                );
            }
        }

        return Status::OK;
    }

private:
    LocalStorage storage;
    sqlHelper helper;
};

#endif