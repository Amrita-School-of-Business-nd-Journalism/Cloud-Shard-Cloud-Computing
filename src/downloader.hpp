#ifndef downloader_H
#define downloader_H
#include <curl/curl.h>
#include <cstdint>
#include <vector>
#include <string>
#include <iostream>

#include "shardMetadata.hpp"
#include "datasetMetadata.hpp"
#include "localStorage.hpp"
#include "sqlHelper.hpp"


static sqlHelper helper;
static ShardMetadata metadata;

        static LocalStorage storage;
constexpr std::size_t BUFFER_SIZE = 1024 * 1024; // 1 MB

class Downloader{
    private:
        struct Buffer {
            std::vector<std::uint8_t> data;
            std::size_t used = 0;
            Buffer(std::uint64_t shard_size): data(shard_size) {}
            Buffer(): data(BUFFER_SIZE) {}
        }; 

        std::size_t shard_index = 0;
        std::uint64_t shard_size;
        // std::string dataset_id_ = "200";


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
};

size_t Downloader::writeCallBack(void* ptr, size_t size, size_t nmemb, void* userdata){
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
            metadata.size_ = buffer->used;
            metadata.print();
            helper.create_shard_data(metadata);
            if(!storage.createShard(metadata.shard_id_,metadata.dataset_id_,metadata.size_,helper)) std::cout<<"Didn't create shard at all"<<std::endl;
            storage.writeShard(metadata.shard_id_,metadata.dataset_id_,0,buffer->data,metadata.size_,helper);
            buffer->used = 0;
            metadata.shard_id_ = std::to_string(std::stoi(metadata.shard_id_)+1);
        }
    }
    
    return incoming_size;
}

bool Downloader::download(const DatasetMetadata& dataset_metadata){
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
        &Downloader::writeCallBack
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
        metadata.size_ = buffer.used;
        metadata.print();
        helper.create_shard_data(metadata);
        storage.createShard(metadata.shard_id_,metadata.dataset_id_,metadata.size_,helper);
        if(!storage.writeShard(metadata.shard_id_,metadata.dataset_id_,0,buffer.data,metadata.size_,helper)) std::cout<<"Failed write"<<std::endl;
        buffer.used = 0;
    }

    curl_easy_cleanup(curl);

    return true;
}

void Downloader::updateCounter(){
    shard_index++;
}

void Downloader::updateMetadata(const ShardMetadata& METADATA){
    metadata = METADATA;
}

#endif

/*
#include <curl/curl.h>
#include <cstdint>
#include <iostream>
#include <array>
#include <cstring>

constexpr std::size_t BUFFER_SIZE = 1024 * 1024; // 1 MB

struct Buffer {
    std::array<std::uint8_t, BUFFER_SIZE> data;
    std::size_t used = 0;
};

size_t writeCallback(
    void* ptr,
    size_t size,
    size_t nmemb,
    void* userdata
) {
    auto* buffer = static_cast<Buffer*>(userdata);

    const std::size_t incomingSize = size * nmemb;

    const auto* incoming =
        static_cast<const std::uint8_t*>(ptr);

    std::size_t position = 0;

    while (position < incomingSize) {

        // How much space is left in our 1 MB buffer?
        std::size_t space =
            BUFFER_SIZE - buffer->used;

        // How much can we copy right now?
        std::size_t toCopy =
            std::min(space, incomingSize - position);

        std::memcpy(
            buffer->data.data() + buffer->used,
            incoming + position,
            toCopy
        );

        buffer->used += toCopy;
        position += toCopy;


        // Buffer is full.
        if (buffer->used == BUFFER_SIZE) {

            std::cout << "1 MB received\n";

            // ------------------------------------
            // PROCESS THE 1 MB HERE
            // ------------------------------------

            // For example:
            //
            // localStorage.writeShard(
            //     ...,
            //     buffer->data,
            //     ...
            // );


            // Reuse the same 1 MB buffer.
            buffer->used = 0;
        }
    }

    return incomingSize;
}


int main() {

    CURL* curl = curl_easy_init();

    if (!curl) {
        return 1;
    }


    Buffer buffer;


    curl_easy_setopt(
        curl,
        CURLOPT_URL,
        "https://example.com/bigfile.bin"
    );


    // ONE HTTP REQUEST.
    //
    // This could be 500 MB.
    //
    curl_easy_setopt(
        curl,
        CURLOPT_RANGE,
        "0-499999999"
    );


    curl_easy_setopt(
        curl,
        CURLOPT_WRITEFUNCTION,
        writeCallback
    );


    curl_easy_setopt(
        curl,
        CURLOPT_WRITEDATA,
        &buffer
    );


    CURLcode result =
        curl_easy_perform(curl);


    if (result != CURLE_OK) {

        std::cerr
            << "Error: "
            << curl_easy_strerror(result)
            << '\n';

        curl_easy_cleanup(curl);

        return 1;
    }


    // There may be some data left that
    // didn't fill the final 1 MB buffer.
    if (buffer.used > 0) {

        std::cout
            << "Final chunk: "
            << buffer.used
            << " bytes\n";

        // Process final partial chunk here.
    }


    curl_easy_cleanup(curl);

    return 0;
}
    
*/