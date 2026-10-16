# CloudShard: A Cloud-Native Distributed Dataset Storage and Streaming Framework for Large-Scale Machine Learning

CloudShard is a distributed dataset storage prototype designed to ingest datasets from supported online sources, divide incoming data into shards, and distribute those shards across multiple storage peers. It combines a Python-based requesting interface with C++ storage peers, local SQLite metadata persistence, peer coordination, and storage-capacity monitoring.

The project explores a shard-oriented approach to organizing and managing datasets for large-scale machine learning workloads.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Build Instructions](#build-instructions)
- [Running the System](#running-the-system)
- [Current Implementation Status](#current-implementation-status)
- [Limitations and Future Work](#limitations-and-future-work)
- [Contributing](#contributing)
- [License](#license)

## Overview

Large datasets used in machine learning can create storage and data-access challenges when they must be organized and distributed across multiple machines. CloudShard explores a peer-based architecture in which dataset data is divided into manageable shards and stored across independent storage peers.

The system separates the requesting side from the storage side:

- **Python requesting peer:** Initiates user-directed operations and interacts with the peer-storage system.
- **C++ storage peers:** Store assigned shard data locally and participate in peer coordination.
- **SQLite metadata:** Maintains local records associating dataset and shard identifiers with storage paths.
- **Peer coordination:** Exchanges peer and storage information to support shard placement and location discovery.
- **Capacity monitoring:** Uses heartbeat information to report peer availability and remaining storage capacity.

The current implementation focuses on ingestion, sharding, peer-based storage, coordination, and metadata management. Complete on-demand file reconstruction and end-to-end integrity verification remain areas for further development.

## Key Features

### 1. Dataset Ingestion

- Ingests data from supported web URLs and direct-download Google Drive URLs.
- Uses libcurl in the C++ ingestion components.
- Supports ingestion subject to the source URL format and download-access requirements.
- Does not imply unrestricted access to private, authenticated, or otherwise unsupported sources.

### 2. Deterministic Sharding and Distributed Placement

- Divides incoming data into fixed-size shard units.
- Supports distributing shards across multiple storage peers.
- Supports user-directed shard-placement decisions.
- Uses peer storage-capacity information to help inform placement.
- Stores shard data on the assigned peers rather than requiring every peer to hold the entire dataset.

### 3. Peer Coordination and Heartbeats

- Supports a multi-peer storage setup.
- Exchanges peer and storage information to help the requesting side identify where data is held.
- Uses heartbeat-based monitoring to report peer availability and remaining capacity.
- Provides a foundation for coordinating storage across independent peer instances.

### 4. Persistent Metadata

SQLite provides local metadata persistence for storage-related information, including dataset and shard identifiers and their corresponding local paths.

Each peer's local metadata supports tracking the shard data stored by that peer. Peer coordination complements this local metadata when information about shards stored elsewhere is required.

### 5. Shard Retrieval Interface

The repository defines a Protocol Buffers/gRPC interface for requesting a shard using its identifiers and a byte range.

The interface includes:

- Dataset identifier
- Shard identifier
- Byte offset
- Requested byte count

The complete client-side retrieval, multi-shard reconstruction, and end-to-end integrity-validation workflow is not yet complete.

## System Architecture

The intended high-level workflow is:

```text
                User Request
                     |
                     v
          Python Requesting Peer
                     |
                     v
           Supported Data Source
          (Web URL / Google Drive)
                     |
                     v
             Dataset Ingestion
                     |
                     v
              Shard Creation
                     |
                     v
        Placement Based on User Input
          and Available Peer Capacity
                     |
          +----------+----------+
          |          |          |
          v          v          v
       C++ Peer A  C++ Peer B  C++ Peer C
          |          |          |
          v          v          v
       Local       Local       Local
       Shards      Shards      Shards
          |          |          |
          v          v          v
       SQLite     SQLite     SQLite
       Metadata   Metadata   Metadata

       Peer coordination and heartbeat
       information support storage discovery.

       Complete retrieval, reconstruction,
       and integrity verification are
       planned extensions.
```

The diagram illustrates the system's architectural workflow. Exact communication paths and component responsibilities depend on the corresponding implementations in the source code.

## Technology Stack

| Technology | Purpose |
|---|---|
| C++17 | Core storage and shard-management components |
| Python | Requesting-side operations |
| CMake | Build configuration |
| libcurl | Dataset download and ingestion |
| SQLite | Persistent local metadata |
| Protocol Buffers | Structured service-message definitions |
| gRPC | Defined shard-retrieval interface |

## Repository Structure

```text
Cloud-Shard-Cloud-Computing/
├── Database/       # Database-related files
├── Node_2_Shards/  # Node/shard-related files
├── Shards/         # Shard storage files
├── proto/          # Protocol Buffers service definitions
├── src/            # C++ source and header files
├── CMakeLists.txt  # Build configuration
├── .gitignore
├── LICENSE
└── README.md
```

The exact contents of each directory may evolve as the implementation develops.

## Prerequisites

The C++ build configuration requires:

- A C++17-compatible compiler
- CMake 3.20 or newer
- Protocol Buffers
- gRPC C++ libraries and code-generation tools
- SQLite3 development libraries
- libcurl development libraries

For a Debian/Ubuntu-based Linux environment, the following packages provide the main build dependencies:

```bash
sudo apt update

sudo apt install -y \
    build-essential \
    cmake \
    pkg-config \
    libsqlite3-dev \
    libcurl4-openssl-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libgrpc++-dev \
    protobuf-compiler-grpc
```

Package availability and CMake package-discovery requirements may vary by operating system and distribution.

Python dependencies, if any, depend on the requesting-peer implementation.

## Build Instructions

Clone the repository:

```bash
git clone https://github.com/Amrita-School-of-Business-nd-Journalism/Cloud-Shard-Cloud-Computing.git

cd Cloud-Shard-Cloud-Computing
```

Configure the project:

```bash
cmake -S . -B build
```

Compile the C++ targets:

```bash
cmake --build build -j
```

The current CMake configuration generates executable targets from the C++ source files and places their outputs in the `Outputs/` directory.

Successful configuration and compilation depend on the installed libraries and the current state of the source files.

## Running the System

CloudShard's peer-based workflow requires the requesting-side component and the relevant C++ storage-peer processes to be configured consistently.

A typical execution sequence is:

1. Configure the supported source URL and ingestion parameters.
2. Start the required C++ storage-peer instances.
3. Verify that peers become available and report their storage capacity.
4. Start the Python requesting-side component using its implemented entry point.
5. Initiate ingestion and specify the desired shard-placement instructions.
6. Verify that the resulting shard files are present on the assigned peers.
7. Inspect local SQLite metadata and peer-coordination information.

**Important:** Use the actual executable names, Python entry point, arguments, and port configuration defined in the source code. These details should be documented explicitly once the runtime configuration has been confirmed; they should not be inferred from the directory names alone.

## Current Implementation Status

| Component | Status |
|---|---|
| Supported URL-based ingestion | Implemented with source constraints |
| Fixed-size shard creation | Implemented |
| User-directed shard placement across peers | Implemented |
| Multiple C++ storage-peer instances | Implemented |
| Heartbeat-based storage-capacity reporting | Implemented |
| Local SQLite metadata persistence | Implemented |
| Peer coordination and storage-information exchange | Implemented |
| Protocol Buffers/gRPC shard-retrieval interface | Defined in the repository |
| Complete on-demand retrieval and file reconstruction | Incomplete |
| End-to-end data integrity verification | Incomplete |

This status describes the reported implementation scope. The presence of an interface definition does not, by itself, establish that its complete runtime workflow is operational.

## Limitations and Future Work

The current project provides a foundation for distributed shard storage and peer coordination. Several capabilities remain to be developed or validated:

- **On-demand retrieval:** Implement a complete workflow that locates and retrieves the shards required for a requested dataset or file.
- **File reconstruction:** Reassemble retrieved data in the correct order and reproduce the original file.
- **Integrity verification:** Compute and validate checksums, such as SHA-256, to detect missing, corrupted, or incorrectly reconstructed data.
- **Failure handling:** Define how the system behaves when a storage peer becomes unavailable during retrieval.
- **Metadata consistency:** Improve cross-peer metadata discovery and stale-location handling as the system evolves.
- **Performance evaluation:** Measure ingestion throughput, shard-placement overhead, retrieval latency, and storage utilization under reproducible test conditions.
- **Scalability evaluation:** Test larger datasets and additional storage peers to characterize system behaviour.

These are future development goals, not claims of completed functionality.

## Contributing

Contributions that improve storage reliability, peer coordination, metadata handling, retrieval, testing, and documentation are welcome.

When extending the project, ensure that implementation claims in the documentation correspond to functionality that has been implemented and tested.

## License

This project includes an MIT license. Refer to the repository's `LICENSE` file for the applicable terms.
