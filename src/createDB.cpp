#include <sqlite3.h>
#include <iostream>
#include <bits/stdc++.h>
using namespace std;

int main(){
    sqlite3* db = nullptr;

    int rc = sqlite3_open("Database/test.db", &db);
    if (rc != SQLITE_OK){
        cout<<"Failed to open database!!"<<endl;
        sqlite3_close(db);
        return 1;
    }

    
        // std::string id_;
        // uint64_t size_;
        // std::string checksum_;

        // std::chrono::system_clock::time_point creationTime_;
        // std::filesystem::path localPath_;

    const char* sql = R"(
        CREATE TABLE ShardMetadata (
            shard_id_ varchar(20) unique not null,
            dataset_id_ varchar(20) unique not null,
            size_ int,
            checksum_ varchar(100),
            created_at_ TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            localpath_ varchar(200),
            primary key(shard_id_, dataset_id_)
        );
    )";

    char* error = nullptr;

    rc = sqlite3_exec(db,sql,nullptr,nullptr,&error);

    if (rc != SQLITE_OK){
        std::cout<<error<<endl;
        sqlite3_free(error);
    }


    sqlite3_close(db);
    return 0;
}