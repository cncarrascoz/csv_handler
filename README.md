# ColumnarDB: A Distributed Column-Oriented Database System

ColumnarDB is a distributed column-oriented database system implemented in C++ that provides efficient storage, processing, and replication of structured data across multiple nodes. It leverages the Raft consensus algorithm to ensure consistency across the distributed cluster.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [System Components](#system-components)
- [Features](#features)
- [Dependencies](#dependencies)
- [Directory Structure](#directory-structure)
- [Building and Running](#building-and-running)
- [Client Commands](#client-commands)
- [Server Commands](#server-commands)
- [Data Operations](#data-operations)
- [Distributed Consensus](#distributed-consensus)
- [State Machine Architecture](#state-machine-architecture)
- [Advanced Features](#advanced-features)
- [Distributed Usage](#distributed-usage)

## Overview

ColumnarDB is a column-oriented database system with distributed capabilities. The system stores data efficiently in columnar format, allowing for rapid analytical operations. A leader-follower architecture using the Raft consensus protocol ensures data consistency and fault tolerance across all nodes in the cluster.

### Key Capabilities

- **Efficient Columnar Storage**: Data is stored in a column-oriented format for optimized analytical processing
- **Distributed Architecture**: Multiple nodes working together with automatic leader election
- **Consensus Protocol**: Implementation of the Raft algorithm for distributed consistency
- **Analytical Operations**: Fast aggregation functions (sum, average) on column data
- **Data Modification**: Insert and delete operations with consistency guarantees
- **gRPC Communication**: Fast, efficient inter-node communication and client connections
- **State Machine Architecture**: Extensible design for different storage implementations

## Architecture

The system is built on a distributed architecture consisting of:

- **Columnar Storage Engine**: Core data storage using column-oriented design
- **State Machine Layer**: Implements the state transitions for data operations
- **Raft Consensus Module**: Manages leader election and log replication
- **gRPC Communication Layer**: Handles client requests and inter-node communication
- **Persistence Layer**: Manages data durability across restarts

### Data Flow

1. Clients connect to any node in the cluster via gRPC
2. Non-leader nodes forward write requests to the leader
3. Leader appends operation to its log and replicates to followers
4. When a majority of nodes confirm, the leader commits the operation
5. All nodes apply committed operations to their local state machines
6. Results are returned to the client

## System Components

The system is organized into the following main components:

- **Core**: Fundamental interfaces and data structures
  - `IStateMachine`: Abstract interface for the state machine
  - `Mutation`: Data structure representing state changes
  - `TableView`: Read-only view of column data
  
- **Storage**: Column-oriented data storage implementation
  - `ColumnStore`: The columnar data structure
  - `InMemoryStateMachine`: In-memory implementation of the state machine
  
- **Distributed**: Consensus and replication implementations
  - `RaftNode`: Implementation of the Raft consensus algorithm
  - Server registry for tracking cluster membership
  
- **Persistence**: Durability mechanisms
  - Write-ahead logging
  - Snapshots for efficient recovery
  
- **Network**: gRPC service implementations
  - Client request handling
  - Inter-node communication

## Features

### Column-Oriented Storage

Data is stored in a columnar format, which offers several advantages:

- Efficient compression of similar column values
- Improved cache efficiency for analytical operations
- Better performance for operations that only need specific columns
- Optimized aggregation operations

### Distributed Consensus

The system implements the Raft consensus algorithm to provide:

- Strong consistency guarantees
- Automatic leader election after failures
- Ordered transaction log replication
- Fault tolerance with majority quorum

### State Machine Architecture

All operations are modeled as state machine transitions:

- The system maintains a replicated log of operations
- Each operation is a deterministic transition
- All nodes independently apply the same operations in the same order
- Multiple state machine implementations for different storage needs

## Dependencies

To build and run this project, you need:

- **C++17 Compiler**: A modern C++ compiler supporting C++17 or later (e.g., GCC, Clang)
- **CMake**: Version 3.10 or higher for building the project
- **gRPC**: The gRPC library and its dependencies (including Protocol Buffers). Installation instructions can be found at [grpc.io](https://grpc.io/docs/languages/cpp/quickstart/)
- **Protocol Buffers**: Version 3.x or higher (usually installed as part of gRPC)

*Note: The specific versions required might depend on your system setup. Ensure compatibility between gRPC and Protobuf.*

## Directory Structure

```
columnardb/
├── build/          # Build directory (created by CMake)
├── client/
│   ├── csv_client.cpp  # Client logic implementation
│   ├── csv_client.hpp  # Client class header
│   ├── main.cpp        # Client executable entry point
│   ├── menu.cpp        # Client menu system implementation
│   └── menu.hpp        # Client menu system header
├── core/           # Core interfaces and data structures
│   ├── IStateMachine.hpp  # State machine interface
│   ├── Mutation.hpp       # Data mutation operations
│   └── TableView.hpp      # Read-only view abstraction
├── data/           # Directory containing sample data files
│   ├── mock_data.csv
│   ├── mock_data1.csv
│   └── test_data.csv
├── distributed/    # Distributed system components
│   ├── raft_node.cpp     # Raft consensus implementation
│   ├── raft_node.hpp     # Raft node interface
│   └── main.cpp          # Test entry point for Raft
├── persistence/    # Data persistence components
│   ├── DurableStateMachine.cpp  # Persistent state machine implementation
│   ├── DurableStateMachine.hpp  # Durable state machine interface
│   ├── Snapshot.cpp             # State snapshot implementation
│   ├── Snapshot.hpp             # Snapshot interface
│   ├── WriteAheadLog.cpp        # Write-ahead log implementation
│   └── WriteAheadLog.hpp        # WAL interface
├── proto/
│   ├── csv_service.proto     # Service definition
│   ├── mutation.proto        # Mutation message definitions
│   ├── cluster_service.proto # Cluster communication service
│   ├── csv_service.pb.cc     # Generated Protobuf C++ source
│   ├── csv_service.pb.h      # Generated Protobuf C++ header
│   ├── csv_service.grpc.pb.cc # Generated gRPC C++ source
│   └── csv_service.grpc.pb.h # Generated gRPC C++ header
├── server/
│   ├── network/
│   │   ├── csv_service_impl.cpp # Server RPC implementation
│   │   ├── csv_service_impl.hpp # Server RPC class header
│   │   ├── server_registry.cpp  # Server registry implementation
│   │   └── server_registry.hpp  # Server registry interface
│   ├── main.cpp                 # Server executable entry point
│   ├── menu.cpp                 # Server menu system implementation
│   └── menu.hpp                 # Server menu system header
├── storage/        # Storage implementations
│   ├── InMemoryStateMachine.cpp  # In-memory state machine implementation
│   ├── InMemoryStateMachine.hpp  # In-memory state machine header
│   ├── column_store.cpp          # Column store implementation
│   ├── column_store.hpp          # Column store interface
│   ├── csv_parser.cpp            # CSV parsing utilities (legacy)
│   └── csv_parser.hpp            # CSV parser interface (legacy)
├── utils/
│   ├── file_utils.cpp # File reading utilities
│   └── file_utils.hpp # Header for file utilities
├── CMakeLists.txt     # CMake build configuration
└── README.md          # This file
```

## Building and Running

### Building

1. **Generate Protocol Buffer Files:**
   * Navigate to the project root directory.
   * Ensure `protoc` and `grpc_cpp_plugin` are in your PATH or provide full paths.
   * Run the following command:
     ```bash
     protoc --proto_path=. --cpp_out=./proto --grpc_out=./proto --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) proto/csv_service.proto proto/mutation.proto proto/cluster_service.proto
     ```
   * This will generate the necessary `.pb.h`, `.pb.cc`, `.grpc.pb.h`, and `.grpc.pb.cc` files inside the `proto/` directory.

2. **Configure with CMake:**
   * Create a build directory and navigate into it:
     ```bash
     mkdir -p build
     cd build
     ```
   * Run CMake to configure the project:
     ```bash
     cmake ..
     ```

3. **Compile:**
   * From the `build` directory, run:
     ```bash
     make
     ```
   * This will create the `server`, `client`, and other executables in the `build` directory.

4. **Clean Build (if needed):**
   * To clean the build and start fresh:
     ```bash
     # From the build directory
     make clean
     
     # For a complete clean (removing all CMake-generated files)
     cd ..
     rm -rf build
     mkdir build
     cd build
     cmake ..
     make
     ```

5. **Rebuild After Changes:**
   * After making changes to source files, simply run:
     ```bash
     # From the build directory
     make
     ```
   * If you've added new source files or changed the CMakeLists.txt:
     ```bash
     # From the build directory
     cmake ..
     make
     ```

### Running a Single Node

To run a standalone server instance:

```bash
./server localhost:50051
```

The server will start listening on `0.0.0.0:50051` (advertised as localhost:50051) by default.

### Running a Cluster

Start multiple server instances with different addresses, specifying peer connections:

```bash
# First node (will be initial leader)
./server localhost:8080

# Second node (specifies first node as peer)
./server localhost:8081 localhost:8080

# Third node (specifies first and second nodes as peers)
./server localhost:8082 localhost:8080 localhost:8081
```

Each server instance needs a unique address (hostname:port). The first argument is the server's own address, and any additional arguments are the addresses of other nodes in the cluster to connect to.

### Running the Client

The client can be run in two modes:

* **Interactive Mode:**
  ```bash
  ./client localhost:50051
  ```

* **Command-Line Mode:**
  ```bash
  ./client localhost:50051 <command> [arguments]
  ```
  Example: `./client localhost:50051 list`

## Client Commands

The system supports the following commands through its interactive client interface:

- **upload \<filename\>**: Upload a data file to the server
- **list**: List all tables stored in the database
- **view \<filename\>**: View the contents of a table
- **sum \<filename\> \<column_name\>**: Calculate the sum of values in a column
- **avg \<filename\> \<column_name\>**: Calculate the average of values in a column
- **insert \<filename\> \<value1\> \<value2\> \<value3\> ...**: Insert a new row with the specified values
  - Example: `insert test13.csv Ruby 1995 Yukihiro_Matsumoto`
  - Note: Values containing spaces should be enclosed in quotes or use underscores
  - The number of values must match the number of columns in the table
- **delete \<filename\> \<row_index\>**: Delete the row at the specified index
- **display \<filename\>**: Open a real-time display of a table in a new terminal, with live updates
- **status**: Show the cluster status and leader information
- **exit**: Exit the program
- **help**: Display help information about available commands

The **display** command opens a new terminal window that shows the table content and automatically updates when the data changes on the server. This provides a real-time view that's useful for monitoring data that's being modified by other clients.

## Server Commands

The server also has an interactive interface with these commands:

- **list**: List all loaded tables with their metadata
- **stats [filename]**: Show statistics for all tables or a specific table
- **status**: Display the current node status, role, and cluster information
- **ip**: Display the server's IP address for remote client connections
- **exit**: Shutdown the server
- **help**: Display help information

## Data Operations

### Mutation Processing

All data-changing operations are treated as mutations that flow through the system:

1. Client sends a mutation request to any node
2. If the receiving node is not the leader, it forwards to the leader
3. Leader appends operation to its log and replicates to followers
4. When a majority of nodes confirm, the leader commits the operation
5. All nodes apply committed operations to their local state machines
6. Results are returned to the client

### Analytical Operations

The system supports various analytical operations:

- **Aggregations**: Sum, average, etc. of numeric columns
- **Filtering**: Select specific rows based on criteria
- **Projections**: Select specific columns for viewing

## Distributed Consensus

### Raft Implementation

The distributed consensus implementation follows the Raft protocol:

- **Leader Election**: Automatic election of a leader node when the previous leader fails
- **Log Replication**: Replication of mutation operations to all follower nodes
- **Safety Guarantees**: Ensures consistency through log matching and election restrictions
- **Membership Changes**: Adding or removing nodes from the cluster

### Leader-Follower Pattern

- The leader node processes all write operations
- Follower nodes forward write requests to the leader
- Read operations can be served by any node (eventual consistency) or by the leader (strong consistency)
- If the leader fails, a new leader is automatically elected

## State Machine Architecture

The system uses a state machine architecture where:

- **State**: The current database with all its tables
- **Mutations**: Operations that transform the state
- **Log**: An ordered sequence of mutations
- **Consensus**: Agreement on the log order
- **Application**: The process of applying mutations to the state

This design enables:

- **Multiple Storage Implementations**: In-memory, durable, or distributed
- **Replay Capability**: The system can rebuild state by replaying the log
- **Snapshot and Recovery**: State can be saved and restored efficiently

## Advanced Features

- **Durability**: Write-ahead logging and snapshots
- **Conflict Resolution**: Handling of concurrent operations
- **Cluster Management**: Dynamic addition and removal of nodes
- **Real-time Notifications**: Clients can subscribe to change events

## Distributed Usage

The system is designed for distributed operation with multiple nodes working in a coordinated cluster:

### Setting Up a Distributed Cluster

1. **Start the Leader Node:**
   * Initialize the first node as a standalone server:
     ```bash
     ./server localhost:50051
     ```
   * This node will initially be a leader with no peers.
   * Use the `list` command in the client to verify the server is running correctly.

2. **Add Follower Nodes:**
   * Start additional nodes and specify the leader address as a peer:
     ```bash
     # On a second terminal
     ./server localhost:50052 localhost:50051
     
     # On a third terminal
     ./server localhost:50053 localhost:50051 localhost:50052
     ```
   * Each server should be started in a separate terminal window.
   * Make sure to use different port numbers for each server instance.

3. **Testing Distributed Functionality:**
   * Connect a client to the leader node:
     ```bash
     ./client localhost:50051
     ```
   * Upload a test CSV file:
     ```
     upload /path/to/your/file.csv
     ```
   * Insert a new row:
     ```
     insert file.csv value1 value2 value3
     ```
   * Verify the insert was successful:
     ```
     view file.csv
     ```
   * Connect another client to a follower node:
     ```bash
     ./client localhost:50052
     ```
   * Verify that the follower has the same data:
     ```
     view file.csv
     ```
   * Test sum and average operations on both nodes to ensure they return the same results:
     ```
     sum file.csv NumericColumn
     avg file.csv NumericColumn
     ```

4. **Troubleshooting:**
   * If nodes can't communicate, ensure there are no firewall issues blocking the ports.
   * If a follower can't connect to the leader, verify the leader is running and the address is correct.
   * If data isn't replicating properly, check the server logs for error messages.
   * For local testing, use different port numbers on localhost (e.g., 50051, 50052, 50053).

### Real-Time Collaborative Features

* All clients connect to the distributed cluster and share the same data.
* When a client uploads data or makes changes (insert/delete rows), those changes are:
  1. Replicated to all nodes in the cluster
  2. Immediately visible to all other connected clients
* Use the `list` command to see all available tables on the cluster.
* Use the `view <filename>` command to see the current state of any table.
* Use the `display <filename>` command for real-time monitoring of tables as they change.
