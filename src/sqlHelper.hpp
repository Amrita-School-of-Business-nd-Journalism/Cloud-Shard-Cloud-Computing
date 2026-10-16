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
        std::size_t getNextShardId(const std::string& dataset_id_);
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

std::size_t sqlHelper::getNextShardId(
    const std::string& dataset_id_
) {
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db,
        "SELECT shard_id_ FROM ShardMetadata "
        "WHERE dataset_id_=? "
        "ORDER BY CAST(shard_id_ AS INTEGER) DESC "
        "LIMIT 1",
        -1, &stmt, nullptr
    );

    if (rc != SQLITE_OK) {
        std::cerr << "Prepare failed: "
                  << sqlite3_errmsg(db) << '\n';
        return 0; // Error sentinel
    }

    sqlite3_bind_text(
        stmt, 1, dataset_id_.c_str(), -1, SQLITE_TRANSIENT
    );

    rc = sqlite3_step(stmt);

    std::size_t output = 1; // First shard ID

    if (rc == SQLITE_ROW) {
        const unsigned char* value = sqlite3_column_text(stmt, 0);

        if (value) {
            try {
                output = std::stoull(
                    reinterpret_cast<const char*>(value)
                ) + 1;
            } catch (const std::exception& e) {
                std::cerr << "Invalid shard ID: "
                          << e.what() << '\n';
                output = 0;
            }
        }
    } else if (rc != SQLITE_DONE) {
        std::cerr << "Query failed: "
                  << sqlite3_errmsg(db) << '\n';
        output = 0;
    }

    sqlite3_finalize(stmt);
    return output;
}

#endif