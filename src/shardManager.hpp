#ifndef shardManager_H
#define shardManager_H
#include <bits/stdc++.h>

#include "shardMetadata.hpp"

class ShardManager{
    public:
        bool storeShard(ShardMetadata& shard_meta);
        bool removeShard(ShardMetadata& shard_meta);
        bool createShard(ShardMetadata& shard_meta);
        std::vector<std::string> listShard();
        std::vector<uint8_t> readShard(ShardMetadata& shard_meta);
        uint64_t freeSpace(ShardMetadata& shard_meta);
        uint64_t usedSpace(ShardMetadata& shard_meta);
        bool shardExist(ShardMetadata& shard_meta);

    private:
        std::filesystem::path folderPath_;

};

bool ShardManager::storeShard(ShardMetadata& shard_meta){
    return true;
}

#endif