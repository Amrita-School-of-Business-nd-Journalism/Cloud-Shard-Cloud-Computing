#ifndef sqlHelper_H
#define sqlHelper_H
#include <string>
#include <sqlite3.h>

class sqlHelper{
    private:
        sqlite3* db = nullptr;
        int rc = sqlite3_open("Database/test.db", &db);
    public:
        std::string getLocation(std::string shard_id_);
};

std::string sqlHelper::getLocation(std::string shard_id_){
    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db, "SELECT localpath_ FROM ShardMetadata WHERE shard_id_=?", -1, &stmt, nullptr);
    sqlite3_bind_text(stmt,1,shard_id_.c_str(),-1,SQLITE_TRANSIENT);
    std::string result;
    if (sqlite3_step(stmt) == SQLITE_ROW){
        const unsigned char* value = sqlite3_column_text(stmt, 0);
        if (value) result = reinterpret_cast<const char*>(value);
    }
    sqlite3_finalize(stmt);
    return result;
}

#endif