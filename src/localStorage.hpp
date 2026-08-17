#ifndef localStorage_H
#define localStorage_H
#include <string>
#include <vector>
#include <cstdint>
#include "sqlHelper.hpp"
#include <filesystem>


// This localStorage class is unique for each client,
//  meaning this class will be present in all the devices
// This class can only access shards present in the same device as itself.
class LocalStorage{

    public:
        // Creating a new shard
        bool createShard(
            const std::string& shard_id_,
            const std::string& dataset_id_,
            const std::uint64_t size
        );

        // Writing in a shard which is already created
        bool writeShard(
            const std::string& shard_id_,
            const std::string& dataset_id_,
            std::uint64_t offset,
            std::vector<std::uint8_t>& data,
            sqlHelper& helper
        );

        // Reading out of a shard
        std::vector<std::uint8_t> readShard(
            const std::string& shard_id_,
            const std::string& dataset_id_,
            std::uint64_t offset,
            const std::uint64_t size
        );

        // Deleting an already existing shard
        bool deleteShard(
            const std::string& shard_id_,
            const std::string& dataset_id_
        );

        // Check whether the shard exists, or basically findShard
        bool shardExist(
            const std::string& shard_id_,
            const std::string& dataset_id_
        ) const;

        // Returns the size of the shard, size of shard is immutable once created
        std::uint64_t shardSize(
            const std::string& shard_id_,
            const std::string& dataset_id_
        ) const;

        // Returns the vector of shardIds in the current node
        std::vector<std::string> listShard();

};

bool LocalStorage::writeShard(const std::string& shard_id_, const std::string& dataset_id_, std::uint64_t offset, std::vector<std::uint8_t>& data, sqlHelper& helper){
    std::filesystem::path temp = helper.getLocation(shard_id_);
    
    return true;
}

#endif