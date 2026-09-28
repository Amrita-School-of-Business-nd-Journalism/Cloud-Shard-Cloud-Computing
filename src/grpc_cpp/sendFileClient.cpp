#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include <memory>

#include <grpcpp/grpcpp.h>

#include "shard.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using shard::ShardReply;
using shard::ShardRequest;
using shard::Shard;


int main()
{
    // Connect to the server
    std::shared_ptr<Channel> channel =
        grpc::CreateChannel(
            "localhost:50051",
            grpc::InsecureChannelCredentials()
        );

    // Create client stub
    unique_ptr<Shard::Stub> stub =
        Shard::NewStub(channel);


    // Build request
    ShardRequest request;

    request.set_shard_id_("1");
    request.set_dataset_id_("PDF");
    request.set_offset(0);
    request.set_size(100);


    // Object where server's response will be placed
    ShardReply reply;

    ClientContext context;


    // Make remote procedure call
    Status status =
        stub->getShard(
            &context,
            request,
            &reply
        );


    if (status.ok())
    {
        // vector<uint8_t> answer;
        // answer.data() = reply.data();
        auto answer = reply.data();
        cout<<"Output received"<<endl;
        for(int i=0;i<answer.size();i++){
            cout<<answer[i]<<"";
        }
        cout<<endl;
    }
    else
    {
        std::cout
            << "RPC failed: "
            << status.error_message()
            << '\n';
    }

    return 0;
}