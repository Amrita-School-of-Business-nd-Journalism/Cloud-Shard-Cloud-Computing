#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include "shardMetadata.hpp"
#include "datasetMetadata.hpp"


#include "localStorage.hpp"
#include "sqlHelper.hpp"

#include "downloader.hpp"

template <class T>
void print(vector<T> s){
    for(int64_t i=0;i<s.size();i++) cout<<static_cast<int>(s[i])<<" ";
    cout<<endl;
}


#include <filesystem>
std::filesystem::path DIRECTORY = "Shards/";


int main(){
    // LocalStorage storage;
    // sqlHelper helper;
    // vector<uint8_t> u_temp = {0,1,2,3};
    // vector<uint8_t> output;

    // // storage.createShard("001","001",1024,helper);
    // // storage.writeShard("001","001",4,u_temp,helper);
    // if(storage.readShard("001","001",4,4,output,helper)) print(output);


    // // Downloader thingy;
    // // thingy.download();

    ShardMetadata metadata;
    sqlHelper helper;
    metadata.shard_id_ = "1";
    metadata.dataset_id_ = "20";
    metadata.size_ = 1024 * 1024;
    // metadata.size_ = 500;
    metadata.checksum_ = "0";
    metadata.localPath_ = DIRECTORY;
    // if(helper.create_shard_data(metadata)) cout<<"Successfully inserted ddata!!"<<endl;
    // else cout<<"Some error or key alredy exist"<<endl;

    DatasetMetadata dataset_metadata;
    dataset_metadata.dataset_id_ = "3";
    dataset_metadata.shard_size = 1024 * 1024;
    // dataset_metadata.link = "https://docs.google.com/presentation/d/1MY2sVEItq_fdGyub6FLcbQf44qE_Ifyks2L8i-xy9ug/edit?usp=sharing";
    dataset_metadata.link = "https://docs.google.com/presentation/d/16FLFuAFqp-Dyr4huzLZT37pei-YB74Tj2AkvQ-NZ3o8/edit?usp=sharing";

    Downloader downloader;
    downloader.updateMetadata(metadata);

    if(downloader.download(dataset_metadata)) cout<<"Finished Running Downloader thingy";
    else cout<<"Error maybe"<<endl;


}