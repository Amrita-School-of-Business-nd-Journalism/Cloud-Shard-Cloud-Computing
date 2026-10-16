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
    metadata.dataset_id_ = "MP4_2";
    metadata.size_ = 1024 * 1024;
    // metadata.size_ = 500;
    metadata.checksum_ = "0";
    metadata.localPath_ = DIRECTORY;
    // if(helper.create_shard_data(metadata)) cout<<"Successfully inserted ddata!!"<<endl;
    // else cout<<"Some error or key alredy exist"<<endl;

    DatasetMetadata dataset_metadata;
    dataset_metadata.dataset_id_ = "MP4";
    dataset_metadata.shard_size = 1024 * 1024;
    // dataset_metadata.link = "https://docs.google.com/presentation/d/1MY2sVEItq_fdGyub6FLcbQf44qE_Ifyks2L8i-xy9ug/edit?usp=sharing";
    // dataset_metadata.link = "https://docs.google.com/presentation/d/16FLFuAFqp-Dyr4huzLZT37pei-YB74Tj2AkvQ-NZ3o8/edit?usp=sharing";
    // dataset_metadata.link = "https://drive.google.com/uc?export=download&id=1-H9mSDkLXSHI0sXTofvCgRpQXdxFaQuB";
    // dataset_metadata.link = "https://drive.google.com/uc?export=download&id=1NLkUPyj8oziQvLUXiebUzQU9csBO5RiIG9dJd74ttcg";
    // dataset_metadata.link = "https://drive.google.com/uc?export=download&id=1LEUxyOjc7hyGf7zUnnfs-kL1j1gHrMmG";
    // dataset_metadata.link = "https://drive.google.com/uc?export=download&id=1JvF1aT9qNUeb2H-3DsRp0xIXYs0HWQrJ";
    dataset_metadata.link = "https://drive.google.com/uc?export=download&id=1qoU4-E__3h4PL_YbEkJh0AzFogxMQvuY";

    Downloader downloader;
    downloader.updateMetadata(metadata);

    if(downloader.download(dataset_metadata)) cout<<"Finished Running Downloader thingy";
    else cout<<"Error maybe"<<endl;


}

// int main(){
//     sqlHelper helper;
//     auto temp = helper.getNextShardId("MP4");
//     cout<<temp<<endl;
    
// }

// #include <curl/curl.h>
// #include <cstdint>
// #include <vector>
// #include <string>
// #include <iostream>

// #include "shardMetadata.hpp"
// #include "datasetMetadata.hpp"
// #include "localStorage.hpp"
// #include "sqlHelper.hpp"


// #include <memory>

// #include <grpcpp/grpcpp.h>
// #include "/home/Sachcith/Sem 5 WSL/Cloud/Project/build/generated/shard.grpc.pb.h"

// bool saveShard(
//     const std::string& server_ip,
//     const std::string& dataset_id,
//     const std::vector<uint8_t>& data
// ) {
//     cout<<server_ip<<endl;
//     // Connect to the server
//     std::shared_ptr<grpc::Channel> channel =
//         grpc::CreateChannel(
//             server_ip,
//             grpc::InsecureChannelCredentials()
//         );

//     // Create client stub
//     std::unique_ptr<shard::Shard::Stub> stub =
//         shard::Shard::NewStub(channel);

//     // Build request
//     shard::ShardData request;
//     request.set_dataset_id_(dataset_id);

//     request.set_data(
//         reinterpret_cast<const char*>(data.data()),
//         data.size()
//     );

//     // Receive server response
//     shard::ShardSaveReply reply;
//     grpc::ClientContext context;

//     grpc::Status status =
//         stub->storeShard(&context, request, &reply);

//     if (status.ok()) {
//         std::cout << reply.message() << '\n';
//         return true;
//     }

//     std::cerr << "Failed to save shard.\n"
//           << "gRPC error code: " << status.error_code() << '\n'
//           << "gRPC error message: " << status.error_message() << '\n'
//           << "gRPC error details: " << status.error_details() << '\n';

//     return false;
// }

// int main(){
//     saveShard("127.0.0.1:43237","Test",{1,2,3,4,5,6,7,8,9});
// }