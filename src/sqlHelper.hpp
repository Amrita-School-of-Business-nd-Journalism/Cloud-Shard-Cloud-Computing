#ifndef sqlHelper_H
#define sqlHelper_H
#include <string>
#include <sqlite3.h>
#include "shardMetadata.hpp"

class sqlHelper{
    private:
        sqlite3* db = nullptr;
        int rc = sqlite3_open("Database/test.db", &db);
    public:
        std::string getLocation(const std::string& shard_id_, const std::string& dataset_id_);
        bool create_shard_data(const ShardMetadata& metadata);
};

std::string sqlHelper::getLocation(const std::string& shard_id_, const std::string& dataset_id_){
    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db, "SELECT localpath_ FROM ShardMetadata WHERE shard_id_=? and dataset_id_=?", -1, &stmt, nullptr);
    sqlite3_bind_text(stmt,1,shard_id_.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt,2,dataset_id_.c_str(),-1,SQLITE_TRANSIENT);
    std::string result;
    if (sqlite3_step(stmt) == SQLITE_ROW){
        const unsigned char* value = sqlite3_column_text(stmt, 0);
        if (value) result = reinterpret_cast<const char*>(value);
    }
    sqlite3_finalize(stmt);
    return result;
}

bool sqlHelper::create_shard_data(const ShardMetadata& metadata){
    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db, "INSERT INTO ShardMetadata(shard_id_, dataset_id_, size_, checksum_, localpath_) VALUES(?,?,?,?,?)", -1, &stmt, nullptr);

    sqlite3_bind_text(stmt,1,metadata.shard_id_.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt,2,metadata.dataset_id_.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt,4,metadata.checksum_.c_str(),-1,SQLITE_TRANSIENT);

    sqlite3_bind_int64(stmt,3,static_cast<sqlite3_int64>(metadata.size_));

    std::string path = metadata.localPath_.string() + "shard_" + metadata.dataset_id_ + "_" + metadata.shard_id_;
    sqlite3_bind_text(stmt,5,path.c_str(),-1,SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if(rc!=SQLITE_DONE){
        std::cerr<<sqlite3_errmsg(db)<<std::endl;
        return false;
    }
    return true;
}

#endif