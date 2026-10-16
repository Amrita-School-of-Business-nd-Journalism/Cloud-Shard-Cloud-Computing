#include <iostream>
#include <bits/stdc++.h>
using namespace std;

#include <memory>

#include <grpcpp/grpcpp.h>

#include "coordinator.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using cloudshard::Coordinator;
using cloudshard::RegisterNodeRequest;
using cloudshard::RegisterNodeResponse;
using cloudshard::HeartbeatRequest;
using cloudshard::HeartbeatResponse;

int main(){
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
    register_request.set_endpoint("127.0.0.1:50053");

    RegisterNodeResponse register_reply;

    ClientContext context;

    Status status = stub->RegisterNode(&context, register_request, &register_reply);

    if(status.ok()){
        cout<<register_reply.message()<<endl;
    }
    else{
        cout<<"Status not ok"<<endl;
    }

    HeartbeatRequest heartbeat_request;

    heartbeat_request.set_node_id("node_2");
    heartbeat_request.set_used_bytes(0);
    heartbeat_request.set_total_bytes(0);

    HeartbeatResponse heartbeat_response;

    ClientContext heartbeat_context;

    status = stub->Heartbeat(&heartbeat_context, heartbeat_request, &heartbeat_response);

    if(status.ok()){
        cout<<heartbeat_response.message()<<endl;
    }
    else{
        cout<<"Status not ok for heartbeat!!"<<endl;
    }
}