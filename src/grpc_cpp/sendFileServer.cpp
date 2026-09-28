#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include "../grpc/sendFile.hpp"

#include <memory>

#include <grpcpp/grpcpp.h>

#include "shard.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;

int main(){
    GetShardService service;

    ServerBuilder builder;

    builder.AddListeningPort(
        "0.0.0.0:50051",
        grpc::InsecureServerCredentials()
    );

    builder.RegisterService(&service);

    std::unique_ptr<Server> server = builder.BuildAndStart();

    cout<<"Server listening in port 50051"<<endl;

    server->Wait();

    return 0;
}