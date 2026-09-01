#ifndef shardMetadata_H
#define shardMetadata_H
#include <string>
#include <filesystem>
#include <chrono>
#include <cstdint>

class ShardMetadata{
    public:
        std::string shard_id_;
        std::string dataset_id_;
        uint64_t size_;
        std::string checksum_;

        std::chrono::system_clock::time_point created_at_;
        std::filesystem::path localPath_;

    // private:

};

#endif