
#ifndef CLOUD_SHARD_SERVICE_HPP
#define CLOUD_SHARD_SERVICE_HPP

#include <memory>
#include <vector>
#include <string>
#include <cstdint>
#include <iostream>
#include <filesystem>

#include <grpcpp/grpcpp.h>
#include "shard.grpc.pb.h"

#include "../localStorage.hpp"
#include "../sqlHelper.hpp"
#include "../shardMetadata.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using shard::Shard;
using shard::ShardData;
using shard::ShardSaveReply;
using shard::ShardRequest;
using shard::ShardReply;
using shard::ListShardsRequest;
using shard::ListShardsReply;

class CloudShardService final : public Shard::Service {
public:
    explicit CloudShardService(std::string localPath)
        : local_path_(std::move(localPath)) {}

    // ---------------------------------------------------------
    // Store a shard
    // ---------------------------------------------------------
    Status storeShard(
        ServerContext* context,
        const ShardData* request,
        ShardSaveReply* reply
    ) override {

        std::cout << "Inside Store Shard" << std::endl;

        const std::string request_data = request->data();

        std::vector<std::uint8_t> shard_data(
            request_data.begin(),
            request_data.end()
        );

        const std::string dataset_id = request->dataset_id_();

        // Generate the next shard ID for this dataset.
        const std::size_t next_id = helper.getNextShardId(dataset_id);
        const std::string shard_id = std::to_string(next_id);

        metadata.shard_id_ = shard_id;
        metadata.dataset_id_ = dataset_id;
        metadata.size_ = shard_data.size();
        metadata.checksum_ = "0";
        metadata.localPath_ = local_path_;

        // Create the shard metadata first.
        bool success = helper.create_shard_data(metadata);

        if (!success) {
            reply->set_status(false);
            reply->set_message("Failed to create shard metadata");

            return Status(
                StatusCode::INTERNAL,
                "Failed to create shard metadata"
            );
        }

        // Create the physical shard.
        success = storage.createShard(
            shard_id,
            dataset_id,
            shard_data.size(),
            helper
        );

        if (!success) {
            reply->set_status(false);
            reply->set_message("Failed to create shard file");

            return Status(
                StatusCode::INTERNAL,
                "Failed to create shard file"
            );
        }

        // Write the shard bytes to disk.
        success = storage.writeShard(
            shard_id,
            dataset_id,
            0,
            shard_data,
            shard_data.size(),
            helper
        );

        if (!success) {
            reply->set_status(false);
            reply->set_message("Failed to write shard data");

            return Status(
                StatusCode::INTERNAL,
                "Failed to write shard data"
            );
        }

        reply->set_status(true);
        reply->set_message("Created Shard Successfully!!");

        std::cout << "Stored shard " << shard_id
                  << " for dataset " << dataset_id
                  << std::endl;

        return Status::OK;
    }

    // ---------------------------------------------------------
    // Read a specific shard or a requested byte range
    // ---------------------------------------------------------
    Status getShard(
        ServerContext* context,
        const ShardRequest* request,
        ShardReply* reply
    ) override {

        std::vector<std::uint8_t> result;

        const bool success = storage.readShard(
            request->shard_id_(),
            request->dataset_id_(),
            request->offset(),
            request->size(),
            result,
            helper
        );

        if (!success) {
            return Status(
                StatusCode::INTERNAL,
                "Failed to read shard from disk"
            );
        }

        if (!result.empty()) {
            reply->set_data(
                reinterpret_cast<const char*>(result.data()),
                result.size()
            );
        } else {
            reply->clear_data();
        }

        return Status::OK;
    }

    // ---------------------------------------------------------
    // List shards belonging to a dataset, including their data
    // ---------------------------------------------------------
    Status listShards(
        ServerContext* context,
        const ListShardsRequest* request,
        ListShardsReply* reply
    ) override {

        const std::string dataset_id = request->dataset_id_();

        const std::vector<std::string> shard_ids =
            storage.listShards(dataset_id, helper);

        for (const auto& shard_id : shard_ids) {
            const std::string shard_path =
                helper.getLocation(shard_id, dataset_id);

            std::error_code ec;
            const auto file_size =
                std::filesystem::file_size(shard_path, ec);

            if (ec) {
                return Status(
                    StatusCode::INTERNAL,
                    "Unable to get size for shard: " + shard_id
                );
            }

            std::vector<std::uint8_t> data;

            if (!storage.readShard(
                    shard_id,
                    dataset_id,
                    0,
                    file_size,
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
    ShardMetadata metadata;
    std::string local_path_;
};

#endif
