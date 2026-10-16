#ifndef splitDownloader_H
#define splitDownloader_H

#include <memory>

#include "shard.grpc.pb.h"

#include <vector>
#include <string>
#include <iostream>

#include "../localStorage.hpp"
#include "../sqlHelper.hpp"
#include "../shardMetadata.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using shard::Shard;
using shard::ShardData;
using shard::ShardSaveReply;

class StoreShardService final : public Shard::Service{
    public:
        Status storeShard(ServerContext* connect, const ShardData* request, ShardSaveReply* reply) override {

            std::string request_data = request->data();
            std::vector<std::uint8_t> shard_data(request_data.begin(),request_data.end());

            std::string dataset_id_ = request->dataset_id_();

            std::size_t temp = helper.getNextShardId(dataset_id_);
            std::string shard_id_ = std::to_string(temp);

            metadata.shard_id_ = shard_id_;
            metadata.dataset_id_ = dataset_id_;
            metadata.size_ = shard_data.size();
            // metadata.checksum_ = 0;
            // metadata.created_at_ = now(); // This created_at_ is automatically created inside sqlHelper!!
            metadata.localPath_ = local_path_;
            
            bool status = helper.create_shard_data(metadata);
            if(status){
                status = storage.createShard(shard_id_,dataset_id_,shard_data.size(),helper);
                if(status){
                    status = storage.writeShard(shard_id_,dataset_id_,0,shard_data,shard_data.size(),helper);
                }
                if(status){
                    reply->set_status(true);
                    reply->set_message("Created Shard Successfully!!");
                    return Status::OK;
                }
                reply->set_status(false);
                reply->set_message("Unable to create shard, but shard_data created Successfully");
                return Status::OK;
            }
            reply->set_status(false);
            reply->set_message("Failed to create shard_data!");
            return Status(
                StatusCode::INTERNAL,
                "Failed to create shard_data"
            );
        }

        StoreShardService(std::string localPath) : local_path_(localPath) {};

    private:
        LocalStorage storage;
        sqlHelper helper;
        ShardMetadata metadata;
        std::string local_path_;
};

#endif