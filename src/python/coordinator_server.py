from concurrent import futures
import time
import grpc

import coordinator_pb2 as coordinator_pb2
import coordinator_pb2_grpc as coordinator_pb2_grpc

import ingest_pb2 as ingest_pb2
import ingest_pb2_grpc as ingest_pb2_grpc

import socket

# Temporary in-memory registry.
# Later, replace this with SQLite.
nodes = {}


def print_network_info():
    hostname = socket.gethostname()

    print(f"\nHostname: {hostname}")
    print(f"Coordinator bind address: 0.0.0.0:50051")

    try:
        # Find the local IP used for the default network route.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()

        print(f"Detected LAN IPv4: {local_ip}")
        print(f"Other laptops should connect to: {local_ip}:50051")

    except OSError as e:
        print(f"Could not detect LAN IPv4: {e}")

    print()


class CoordinatorService(coordinator_pb2_grpc.CoordinatorServicer):

    def RegisterNode(self, request, context):
        node_id = request.node_id.strip()
        endpoint = request.endpoint.strip()

        if not node_id or not endpoint:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Node ID and endpoint are required"
            )

        if node_id in nodes:
            return coordinator_pb2.RegisterNodeResponse(
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

        return coordinator_pb2.RegisterNodeResponse(
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

        return coordinator_pb2.HeartbeatResponse(
            accepted=True,
            message="Heartbeat received"
        )


def start_grpc_server():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    coordinator_pb2_grpc.add_CoordinatorServicer_to_server(
        CoordinatorService(), server
    )

    server.add_insecure_port("0.0.0.0:50051")
    server.start()

    print("Coordinator listening on port 50051")
    server.wait_for_termination()


def ingest_dataset_client():
    print("\n--- Dataset Ingestion Client ---")

    # Get dataset information from the user.
    dataset_id = input("Enter dataset ID: ").strip()
    zip_url = input("Enter ZIP download URL: ").strip()

    if not dataset_id or not zip_url:
        print("Dataset ID and ZIP URL are required.")
        return

    # Collect currently registered, online nodes.
    available_nodes = [
        node["endpoint"]
        for node in nodes.values()
        if node["online"]
    ]

    if not available_nodes:
        print("No online nodes are currently registered.")
        return

    print("\nAvailable nodes:")
    for index, endpoint in enumerate(available_nodes, start=1):
        print(f"{index}. {endpoint}")

    # Ask the user for a shard count for each node.
    shard_counts = []

    print("\nEnter the number of shards for each node:")

    for endpoint in available_nodes:
        while True:
            try:
                count = int(
                    input(f"Shards for {endpoint}: ").strip()
                )

                if count < 0:
                    print("Shard count cannot be negative.")
                    continue

                shard_counts.append(count)
                break

            except ValueError:
                print("Please enter a valid integer.")

    # Connect to the separate dataset-ingestion gRPC server.
    # Replace this address with the actual server's IP and port.
    ingestion_server_address = "localhost:50060" # Ingest ip, needs to be changed for differnet thingy.

    try:
        with grpc.insecure_channel(
            ingestion_server_address
        ) as channel:

            stub = ingest_pb2_grpc.IngestDatasetStub(channel)

            request = ingest_pb2.IngestDatasetRequest(
                dataset_id=dataset_id,
                zip_url=zip_url,
                node_endpoints=available_nodes,
                shard_counts=shard_counts,
            )

            # Replace IngestDataset with the actual RPC method
            # name declared in your .proto service.
            response = stub.downloadDataset(
                request,
                # timeout=30
            )

            print("\nDataset ingestion request sent successfully.")
            print("Response:", response)

    except grpc.RpcError as error:
        print("\nFailed to contact the ingestion server.")
        print("Status:", error.code())
        print("Details:", error.details())


if __name__ == "__main__":
    print_network_info()
    time.sleep(2)

    import threading

    server_thread = threading.Thread(
        target=start_grpc_server,
        daemon=True
    )
    server_thread.start()

    print("\nCoordinator is running.")
    print("Waiting for nodes to register...")
    print("Type 'start' to ingest a dataset.")
    print("Type 'nodes' to list registered nodes.")
    print("Type 'exit' to stop the program.\n")

    try:
        while True:
            command = input("Command: ").strip().lower()

            if command == "start":
                ingest_dataset_client()

            elif command == "nodes":
                if not nodes:
                    print("No nodes registered yet.")
                else:
                    print("\nRegistered nodes:")
                    for node_id, node in nodes.items():
                        status = (
                            "ONLINE" if node["online"]
                            else "OFFLINE"
                        )
                        print(
                            f"{node_id}: {node['endpoint']} "
                            f"[{status}]"
                        )

            elif command == "exit":
                print("Shutting down coordinator...")
                break

            elif command:
                print("Unknown command. Use start, nodes, or exit.")

    except (KeyboardInterrupt, EOFError):
        print("\nExiting coordinator.")
