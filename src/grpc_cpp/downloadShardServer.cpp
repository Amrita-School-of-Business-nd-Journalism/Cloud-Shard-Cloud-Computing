#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include "../downloaderSplit.hpp"
#include "../grpc/downloadShard.hpp"

#include <memory>

#include <grpcpp/grpcpp.h>

#include "ingest.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;

int main(){
    DownloadShardService service;

    ServerBuilder builder;

    builder.AddListeningPort(
        "0.0.0.0:50060",
        grpc::InsecureServerCredentials()
    );

    builder.RegisterService(&service);

    std::unique_ptr<Server> server = builder.BuildAndStart();

    cout<<"This is the server for Ingestion, enter the below ip+port inplace of python ingestion ip"<<endl;
    cout<<"Server listening in port 50060"<<endl;

    server->Wait();

    return 0;
}