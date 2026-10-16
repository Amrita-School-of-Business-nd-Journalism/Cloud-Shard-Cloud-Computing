import grpc

import coordinator_pb2 as pb2
import coordinator_pb2_grpc as pb2_grpc


def main():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = pb2_grpc.CoordinatorStub(channel)

        registration = stub.RegisterNode(
            pb2.RegisterNodeRequest(
                node_id="node_1",
                endpoint="127.0.0.1:50052"
            ),
            timeout=3
        )

        print("Registration:", registration.message)

        heartbeat = stub.Heartbeat(
            pb2.HeartbeatRequest(
                node_id="node_1",
                used_bytes=1024,
                total_bytes=10240
            ),
            timeout=3
        )

        print("Heartbeat:", heartbeat.message)


if __name__ == "__main__":
    main()