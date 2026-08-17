#ifndef localStorage_H
#define localStorage_H
#include <string>
#include <vector>
#include <cstdint>
#include "sqlHelper.hpp"
#include <filesystem>
#include <fstream>


// This localStorage class is unique for each client,
//  meaning this class will be present in all the devices
// This class can only access shards present in the same device as itself.
class LocalStorage{

    public:
        // Creating a new shard
        bool createShard(
            const std::string& shard_id_,
            const std::string& dataset_id_,
            const std::uint64_t size,
            sqlHelper& helper
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
        bool readShard(
            const std::string& shard_id_,
            const std::string& dataset_id_,
            std::uint64_t offset,
            const std::uint64_t size,
            std::vector<std::uint8_t>& output,
            sqlHelper& helper
        );

        // Deleting an already existing shard
        bool deleteShard(
            const std::string& shard_id_,
            const std::string& dataset_id_
        );

        // Check whether the shard exists, or basically findShard
        bool shardExist(
            const std::string& shard_id_,
            const std::string& dataset_id_,
            sqlHelper& helper
        );

        // Returns the size of the shard, size of shard is immutable once created
        std::uint64_t shardSize(
            const std::string& shard_id_,
            const std::string& dataset_id_
        );

        // Returns the vector of shardIds in the current node
        std::vector<std::string> listShard();

};


bool LocalStorage::createShard(const std::string& shard_id_, const std::string& dataset_id_, const std::uint64_t size, sqlHelper& helper){
    std::filesystem::path path = helper.getLocation(shard_id_);

    try{
        if(!std::filesystem::exists(path)){
            std::ofstream file(path, std::ios::binary);
            if(!file.is_open()){
                return false;
            }
            file.close();
        }
        std::filesystem::resize_file(path, size);
        return true;
    }
    catch (const std::filesystem::filesystem_error&){
        return false;
    }
}

bool LocalStorage::writeShard(const std::string& shard_id_, const std::string& dataset_id_, std::uint64_t offset, std::vector<std::uint8_t>& data, sqlHelper& helper){
    std::filesystem::path path = helper.getLocation(shard_id_);
    

    try{
        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);

        if(!std::filesystem::exists(path) || !file.is_open()) return false;

        file.seekp(static_cast<std::streamoff>(offset), std::ios::beg);

        if(!file.good()) return false;

        file.write(
            reinterpret_cast<const char *>(data.data()),
            static_cast<std::streamsize>(data.size())
        );

        if(!file.good()) return false;

        file.flush();

        return file.good();
    }
    catch (const std::filesystem::filesystem_error&){
        return false;
    }
    return true;
}

bool LocalStorage::readShard(const std::string& shard_id_, const std::string& dataset_id_, std::uint64_t offset, const std::uint64_t size, std::vector<std::uint8_t>& output, sqlHelper& helper){
    std::filesystem::path path = helper.getLocation(shard_id_);

    try{
        std::ifstream file(path, std::ios::binary);

        if(!std::filesystem::exists(path) || !file.is_open()) return false;

        file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);

        output.resize(size);

        file.read(
            reinterpret_cast<char *>(output.data()),
            static_cast<std::streamsize>(size)
        );

        if (file.gcount()!=static_cast<std::streamsize>(size)){
            output.clear();
            return false;
        }
        return true;
    }
    catch (const std::filesystem::filesystem_error&){
        return false;
    }
    return true;
}

bool shardExist(const std::string& shard_id_, const std::string& dataset_id_, sqlHelper& helper){
    std::filesystem::path path = helper.getLocation(shard_id_);
    try{
        std::ifstream file(path, std::ios::binary);
        if(!std::filesystem::exists(path) || !file.is_open()) return false;
        file.close();
        return true;
    }
    catch (const std::filesystem::filesystem_error&){
        return false;
    }
}

#endif