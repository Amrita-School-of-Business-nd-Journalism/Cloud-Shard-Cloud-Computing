from concurrent import futures
import time
import grpc

import coordinator_pb2 as pb2
import coordinator_pb2_grpc as pb2_grpc


# Temporary in-memory registry.
# Later, replace this with SQLite.
nodes = {}


class CoordinatorService(pb2_grpc.CoordinatorServicer):

    def RegisterNode(self, request, context):
        node_id = request.node_id.strip()
        endpoint = request.endpoint.strip()

        if not node_id or not endpoint:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Node ID and endpoint are required"
            )

        if node_id in nodes:
            return pb2.RegisterNodeResponse(
                success=True,
                message="Node is already registered",
                already_registered=True
            )

        nodes[node_id] = {
            "endpoint": endpoint,
            "last_seen": time.time(),
            "online": True,
            "used_bytes": 0,
            "total_bytes": 0,
        }

        print(f"Registered node: {node_id} at {endpoint}")

        return pb2.RegisterNodeResponse(
            success=True,
            message="Node registered successfully",
            already_registered=False
        )

    def Heartbeat(self, request, context):
        node_id = request.node_id

        if node_id not in nodes:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Node is not registered"
            )

        node = nodes[node_id]
        node["last_seen"] = time.time()
        node["online"] = True
        node["used_bytes"] = request.used_bytes
        node["total_bytes"] = request.total_bytes

        return pb2.HeartbeatResponse(
            accepted=True,
            message="Heartbeat received"
        )


def start_grpc_server():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    pb2_grpc.add_CoordinatorServicer_to_server(
        CoordinatorService(), server
    )

    server.add_insecure_port("[::]:50051")
    server.start()

    print("Coordinator listening on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    start_grpc_server()