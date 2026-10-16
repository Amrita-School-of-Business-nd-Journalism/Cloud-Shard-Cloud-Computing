#ifndef downloadShard_H
#define downloadShard_H

#include <memory>

#include "ingest.grpc.pb.h"

#include <vector>
#include <string>

#include "../localStorage.hpp"
#include "../sqlHelper.hpp"
#include "../downloaderSplit.hpp"

#include "../shardMetadata.hpp"
#include "../datasetMetadata.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using ingest::IngestDataset;
using ingest::IngestDatasetRequest;
using ingest::IngestDatasetResponse;

class DownloadShardService final : public IngestDataset::Service{
    public:
        Status downloadDataset(ServerContext* connect, const IngestDatasetRequest* request, IngestDatasetResponse* reply) override {
            std::string dataset_id = request->dataset_id();
            std::string zip_url = request->zip_url();

            std::vector<std::string> node_endpoints;
            std::vector<uint64_t> shard_counts;

            for (int i = 0; i < request->node_endpoints_size(); i++) {
                node_endpoints.push_back(request->node_endpoints(i));
            }

            for (int i = 0; i < request->shard_counts_size(); i++) {
                shard_counts.push_back(request->shard_counts(i));
            }

            ShardMetadata metadata;
            sqlHelper helper;
            metadata.dataset_id_ = dataset_id;
            metadata.size_ = 1024*1024;

            DatasetMetadata dataset_metadata;
            dataset_metadata.dataset_id_ = dataset_id;
            dataset_metadata.shard_size = 1024*1024;
            dataset_metadata.link = zip_url;

            DownloaderSplit downloader(
                node_endpoints,
                shard_counts
            );
            downloader.updateMetadata(metadata);

            if(downloader.download(dataset_metadata)) cout<<"Finished Running Split Downloader!!"<<endl;
            else{
                return Status(
                    StatusCode::INTERNAL,
                    "Failed to split download or stopped mid download!!"
                );
            }

            return Status::OK;
        }
};

#endif