#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include "../grpc/saveFile.hpp"

#include <memory>

#include <grpcpp/grpcpp.h>

#include "shard.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;

int main(){
    StoreShardService service("Node_2_Shards/");

    ServerBuilder builder;

    builder.AddListeningPort(
        "0.0.0.0:50054",
        grpc::InsecureServerCredentials()
    );

    builder.RegisterService(&service);

    std::unique_ptr<Server> server = builder.BuildAndStart();

    cout<<"Server listening in port 50054"<<endl;

    server->Wait();

    return 0;
}