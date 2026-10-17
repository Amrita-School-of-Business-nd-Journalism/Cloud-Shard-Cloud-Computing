#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include <memory>

#include <grpcpp/grpcpp.h>

#include "coordinator.grpc.pb.h"
#include "shard.grpc.pb.h"

// #include "../grpc/saveFile.hpp"
// #include "../grpc/sendFile.hpp"
#include "../grpc/cloudShardService.hpp"

#include <thread>
#include <chrono>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using cloudshard::Coordinator;
using cloudshard::RegisterNodeRequest;
using cloudshard::RegisterNodeResponse;
using cloudshard::HeartbeatRequest;
using cloudshard::HeartbeatResponse;

using grpc::Server;
using grpc::ServerBuilder;

int main(){
    CloudShardService service("Node_2_Shards/");

    ServerBuilder builder;

    int selected_port = 0;

    builder.AddListeningPort(
        "0.0.0.0:0",
        grpc::InsecureServerCredentials(),
        &selected_port
    );

    builder.RegisterService(&service);

    std::unique_ptr<Server> server = builder.BuildAndStart();

    if (!server || selected_port == 0) {
        std::cerr << "Failed to start gRPC server!" << std::endl;
        return 1;
    }


    // Connect to the server
    std::shared_ptr<Channel> channel =
        grpc::CreateChannel(
            "localhost:50051",
            grpc::InsecureChannelCredentials()
        );

    // Create client stub
    unique_ptr<Coordinator::Stub> stub =
        Coordinator::NewStub(channel);
        
    RegisterNodeRequest register_request;

    register_request.set_node_id("node_2");
    register_request.set_endpoint("127.0.0.1:"+std::to_string(selected_port));

    RegisterNodeResponse register_reply;

    ClientContext context;

    Status status = stub->RegisterNode(&context, register_request, &register_reply);

    if(status.ok()){
        cout<<register_reply.message()<<endl;
    }
    else{
        cout<<"Status not ok"<<endl;
    }

    
    while (true) {
        HeartbeatRequest heartbeat_request;

        heartbeat_request.set_node_id("node_2");
        heartbeat_request.set_used_bytes(0);
        heartbeat_request.set_total_bytes(0);

        HeartbeatResponse heartbeat_response;
        ClientContext heartbeat_context;

        Status heartbeat_status = stub->Heartbeat(
            &heartbeat_context,
            heartbeat_request,
            &heartbeat_response
        );

        if (heartbeat_status.ok()) {
            // cout << "Heartbeat: "
            //     << heartbeat_response.message() << endl;
        } else {
            cerr << "Heartbeat failed: "
                << heartbeat_status.error_message() << endl;
        }

        std::this_thread::sleep_for(
            std::chrono::seconds(1)
        );
    }


    

    if (!server || selected_port == 0) {
        std::cerr << "Failed to start gRPC server!" << std::endl;
        return 1;
    }

    std::cout << "Server listening on port "
            << selected_port << std::endl;

    server->Wait();

}