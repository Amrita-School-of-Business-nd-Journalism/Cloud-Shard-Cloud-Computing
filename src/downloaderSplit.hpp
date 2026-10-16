#ifndef downloaderSplit_H
#define downloaderSplit_H
#include <curl/curl.h>
#include <cstdint>
#include <vector>
#include <string>
#include <iostream>

#include "shardMetadata.hpp"
#include "datasetMetadata.hpp"
#include "localStorage.hpp"
#include "sqlHelper.hpp"


#include <memory>

#include <grpcpp/grpcpp.h>
#include "shard.grpc.pb.h"

bool saveShard(
    const std::string& server_ip,
    const std::string& dataset_id,
    const std::vector<uint8_t>& data
) {
    cout<<server_ip<<endl;
    // Connect to the server
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateChannel(
            server_ip,
            grpc::InsecureChannelCredentials()
        );

    // Create client stub
    std::unique_ptr<shard::Shard::Stub> stub =
        shard::Shard::NewStub(channel);

    // Build request
    shard::ShardData request;
    request.set_dataset_id_(dataset_id);

    request.set_data(
        reinterpret_cast<const char*>(data.data()),
        data.size()
    );

    // Receive server response
    shard::ShardSaveReply reply;
    grpc::ClientContext context;

    grpc::Status status =
        stub->storeShard(&context, request, &reply);

    if (status.ok()) {
        std::cout << reply.message() << '\n';
        return true;
    }

    std::cerr << "Failed to save shard.\n"
          << "gRPC error code: " << status.error_code() << '\n'
          << "gRPC error message: " << status.error_message() << '\n'
          << "gRPC error details: " << status.error_details() << '\n';

    return false;
}


static sqlHelper helper;
static ShardMetadata metadata;

static LocalStorage storage;
constexpr std::size_t BUFFER_SIZE = 1024 * 1024; // 1 MB


static std::vector<std::string> node_endpoints;
static std::vector<std::uint64_t> shard_counts;
static std::uint64_t current_index;
static std::uint64_t current_count;

class DownloaderSplit{
    private:
        struct Buffer {
            std::vector<std::uint8_t> data;
            std::size_t used = 0;
            Buffer(std::uint64_t shard_size): data(shard_size) {}
            Buffer(): data(BUFFER_SIZE) {}
        }; 

        std::size_t shard_index = 0;
        std::uint64_t shard_size;

        void updateCounter();
    
    public:

        static size_t writeCallBack(
            void* ptr,
            size_t size,
            size_t nmemb,
            void* userdata
        );

        bool download(const DatasetMetadata& dataset_metadata);
        void updateMetadata(const ShardMetadata& METADATA);

        DownloaderSplit(const std::vector<std::string>& node_endpts, const std::vector<std::uint64_t> sh_counts){
            node_endpoints = node_endpts;
            shard_counts = sh_counts;
            current_index = 0;
            current_count = 0;

            for(std::size_t i=1;i<shard_counts.size();i++){
                shard_counts[i] += shard_counts[i-1]; // Prefix sum
            }
        }
};

size_t DownloaderSplit::writeCallBack(void* ptr, size_t size, size_t nmemb, void* userdata){
    auto* buffer = static_cast<Buffer*>(userdata);

    const std::size_t incoming_size = size * nmemb;

    const auto* incoming = static_cast<const std::uint8_t*>(ptr);

    std::size_t position = 0;

    while(position < incoming_size){
        std::size_t space = BUFFER_SIZE - buffer->used;
        std::size_t toCopy = std::min(space, incoming_size - position);

        std::memcpy(
            buffer->data.data() + buffer->used,
            incoming + position,
            toCopy
        );

        buffer->used += toCopy;
        position += toCopy;

        if(buffer->used == BUFFER_SIZE){
            // metadata.size_ = buffer->used;
            // metadata.print();
            // helper.create_shard_data(metadata);
            // if(!storage.createShard(metadata.shard_id_,metadata.dataset_id_,metadata.size_,helper)) std::cout<<"Didn't create shard at all"<<std::endl;
            // storage.writeShard(metadata.shard_id_,metadata.dataset_id_,0,buffer->data,metadata.size_,helper);
            // buffer->used = 0;
            // metadata.shard_id_ = std::to_string(std::stoi(metadata.shard_id_)+1);
            std::vector<std::uint8_t> final_shard(
                buffer->data.begin(),
                buffer->data.begin() + buffer->used
            );

            bool status = saveShard(
                node_endpoints[current_index],
                metadata.dataset_id_,
                final_shard
            );

            if (status) {
                std::cout << "Shard saved successfully!\n";
            }
            else {
                std::cout<<"Shard error"<<std::endl;
            }

            current_count++;
            if(current_count>=shard_counts[current_index] && current_index<shard_counts.size()){
                current_index++;
            }
            buffer->used = 0;

        }
    }
    
    return incoming_size;
}

bool DownloaderSplit::download(const DatasetMetadata& dataset_metadata){
    CURL* curl = curl_easy_init();
    if(!curl) return false;

    // this->shard_size = shard_size;

    Buffer buffer(BUFFER_SIZE);

    curl_easy_setopt(
        curl,
        CURLOPT_URL,
        dataset_metadata.link.c_str()
    );

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    // curl_easy_setopt(curl, CURLOPT_USERAGENT, "Mozilla/5.0");

    // curl_easy_setopt(
    //     curl,
    //     CURLOPT_RANGE,
    //     std::to_string(dataset_metadata.start_range) + "-" + std::to_string(dataset_metadata.end_range)
    // );

    curl_easy_setopt(
        curl,
        CURLOPT_WRITEFUNCTION,
        &DownloaderSplit::writeCallBack
    );

    curl_easy_setopt(
        curl,
        CURLOPT_WRITEDATA,
        &buffer
    );

    CURLcode result = curl_easy_perform(curl);

    if (result != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: "
              << curl_easy_strerror(result)
              << '\n';

        curl_easy_cleanup(curl);
        return false;
    }
    std::cout<<"Buffer used:"<<buffer.used<<std::endl;
    if (buffer.used > 0) {
        // metadata.size_ = buffer.used;
        // metadata.print();
        // helper.create_shard_data(metadata);
        // storage.createShard(metadata.shard_id_,metadata.dataset_id_,metadata.size_,helper);
        // if(!storage.writeShard(metadata.shard_id_,metadata.dataset_id_,0,buffer.data,metadata.size_,helper)) std::cout<<"Failed write"<<std::endl;
        // buffer.used = 0;
        std::vector<std::uint8_t> final_shard(
            buffer.data.begin(),
            buffer.data.begin() + buffer.used
        );

        bool success = saveShard(
            node_endpoints[current_index],
            metadata.dataset_id_,
            final_shard
        );
        buffer.used = 0;

    }

    curl_easy_cleanup(curl);

    return true;
}

void DownloaderSplit::updateCounter(){
    shard_index++;
}

void DownloaderSplit::updateMetadata(const ShardMetadata& METADATA){
    metadata = METADATA;
}

#endif