#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include <memory>

#include <grpcpp/grpcpp.h>

#include "shard.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using shard::ShardData;
using shard::ShardSaveReply;
using shard::Shard;

int main(){
    // Connect to the server
    std::shared_ptr<Channel> channel =
        grpc::CreateChannel(
            "localhost:50054",
            grpc::InsecureChannelCredentials()
        );

    // Create client stub
    unique_ptr<Shard::Stub> stub =
        Shard::NewStub(channel);


    // Build request
    ShardData request;

    request.set_dataset_id_("PDF");

    vector<uint8_t> temp_data = {1,2,3,4,5};

    request.set_data(
        reinterpret_cast<const char*>(temp_data.data()),
        temp_data.size()
    );

    // Object where server's response will be placed
    ShardSaveReply reply;

    ClientContext context;

    Status status = stub->storeShard(&context, request, &reply);

    if(status.ok()){
        cout<<reply.message()<<endl;
    }
    else{
        cout<<"Didn't Receive any message, status not ok!!"<<endl;
    }
}