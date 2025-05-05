# gRPC CSV Handler

## 1. Project Overview

This project implements a distributed column-oriented database system in C++ using gRPC and Protocol Buffers. The system uses a Raft-inspired consensus algorithm for fault tolerance and high availability, allowing data to be replicated across multiple servers in a cluster.

Key features of the system include:

- **Distributed Architecture:** Multiple servers form a cluster with automatic leader election and data replication.
- **Columnar Storage:** Data is stored in a column-oriented format optimized for analytical queries.
- **State Machine Architecture:** A clean abstraction that separates the consensus logic from the storage implementation.
- **Client-Server Communication:** Clients can connect to any server in the cluster, with automatic request forwarding to the current leader.
- **Fault Tolerance:** The system can continue operating even if some servers fail, with automatic leader failover.
- **Extensible Command Interface:** A modular, extensible command interface for data manipulation and analysis.

## 2. System Architecture

The system follows a layered architecture with clear separation of concerns:

### Core Architecture

- **Distributed Columnstore Database:** The system has evolved from a simple CSV handler to a distributed column-oriented database with fault tolerance and high availability.

- **State Machine Architecture:** The system uses a state machine abstraction (`IStateMachine` interface) that allows for different implementations while maintaining a consistent interface for the server logic:

```cpp
class IStateMachine {
public:
    virtual void apply(const Mutation& mutation) = 0;
    virtual TableView view(const std::string& file) const = 0;
    virtual ~IStateMachine() = default;
};
```

This approach enables three key implementations:
1. **InMemoryStateMachine**: The primary implementation that stores data in memory.
2. **DurableStateMachine**: An implementation that uses the Write-Ahead Log (WAL) and snapshots for durability.
3. **Distributed State Machine**: Achieved through the leader-follower model where mutations are replicated across servers.

- **Raft-inspired Consensus:** A simplified version of the Raft consensus algorithm is used for leader election and data replication across the cluster. This ensures consistency and fault tolerance.

- **Client-Server Model:** A standard client-server architecture where clients connect to any server in the cluster. Non-leader servers automatically forward requests to the current leader.

- **Command Dispatch System:** A flexible menu-based command system in both client and server that maps command strings to handler functions.

- **Communication:** gRPC is used for defining the service interface (`proto/csv_service.proto`) and handling remote procedure calls between clients and servers, as well as inter-server communication.

- **Columnar Storage:** Data is stored in a column-oriented format (map of column names to vectors of values), optimized for analytical queries.

### Layered Design

The codebase is organized into logical layers:

#### Core Layer
- `core/IStateMachine.hpp`: Abstract interface for state management
- `core/Mutation.hpp`: Defines data modification operations
- `core/TableView.hpp`: Read-only view abstraction

#### Storage Layer
- `storage/InMemoryStateMachine.hpp/cpp`: In-memory implementation of IStateMachine
- Replaced previous `column_store` implementation with a more extensible design

#### Persistence Layer
- `persistence/WriteAheadLog.hpp/cpp`: Log for recording mutations before they're applied
- `persistence/Snapshot.hpp/cpp`: Mechanism for capturing and restoring state
- `persistence/DurableStateMachine.hpp/cpp`: Persistent state machine implementation

#### Distributed Layer
- `distributed/raft_node.hpp/cpp`: Implementation of the Raft consensus algorithm
- `distributed/heartbeat.hpp/cpp`: Node health monitoring and communication

### Design Patterns

The project follows these key architectural principles:

1. **Command Pattern**: Commands are represented as strings mapped to handler functions, making it easy to add new commands.

2. **Separation of Concerns**:
   - `menu.cpp/hpp`: User interface handling
   - `csv_client.cpp/hpp` & `csv_service_impl.cpp/hpp`: Network communication
   - `column_store.cpp/hpp`: Data storage and analytics
   - `csv_parser.cpp/hpp`: Parsing and data conversion
   - `file_utils.cpp/hpp`: File handling and output formatting

3. **Extensibility**: New commands can be added by:
   - Creating new handler functions
   - Registering them with the menu system
   - Implementing any required backend logic

4. **User Interface Enhancements**:
   - Table formatting for CSV data display with:
     - Plus signs (+) at corners and intersections
     - Dashes (-) for horizontal borders
     - Vertical bars (|) for column separators
     - Proper spacing and alignment of data

### Distributed System Components

#### Mutation System

The system uses a custom `Mutation` struct to represent state-changing operations:

```cpp
struct Mutation {
    std::string file;
    std::variant<RowInsert, RowDelete> op;
    
    bool has_insert() const { return std::holds_alternative<RowInsert>(op); }
    bool has_delete() const { return std::holds_alternative<RowDelete>(op); }
};
```

This design provides type safety and extensibility for adding new mutation types in the future.

#### Write-Ahead Log (WAL)

The system includes a fully implemented Write-Ahead Log (WAL) that enables durability:

- Mutations are first written to the log before being applied to the state machine
- This ensures that no data is lost in case of system crashes
- The WAL can be replayed during system restart to recover the state
- Fully implemented and operational in the current version

#### Snapshot Mechanism

For efficient recovery and state transfer, a snapshot mechanism is fully implemented:

- Periodically captures the entire state of the system
- Allows for faster recovery than replaying the entire log
- Serves as a baseline for new nodes joining the cluster
- Snapshots are automatically created after a configurable number of mutations
- Fully implemented and operational in the current version

#### Distributed Consensus with Raft

The system implements distributed operation using a Raft-inspired consensus algorithm:

- Leader election and automatic failover
- Data replication across multiple nodes
- Support for different server roles (LEADER, FOLLOWER)
- Health monitoring and communication between nodes
- State replication across multiple nodes for fault tolerance

#### Benefits

This architecture provides several benefits:

1. **Reliability**: Durability through the WAL and snapshots
2. **Scalability**: Distribute load across multiple nodes
3. **Fault Tolerance**: Continue operation even if some nodes fail
4. **Consistency**: Strong consistency guarantees through Raft consensus
5. **Backward Compatibility**: All existing client functionality continues to work

## 3. Features

### Client Commands
The system supports the following commands through its interactive menu interface:

- **upload \<filename\>**: Upload a CSV file to the server
- **list**: List all files stored on the server
- **view \<filename\>**: View the contents of a CSV file stored on the server
- **sum \<filename\> \<column_name\>**: Calculate the sum of values in a column
- **avg \<filename\> \<column_name\>**: Calculate the average of values in a column
- **insert \<filename\> \<values\>**: Insert a new row into an existing file
- **delete \<filename\> \<row_index\>**: Delete a row from a file
- **display \<filename\>**: Open a real-time display of a file in a new terminal, with live updates
- **exit**: Exit the program
- **help**: Display help information about available commands

The **display** command opens a new terminal window that shows the file content and automatically updates when the file changes on the server. This provides a real-time view that's useful for monitoring data that's being modified by other clients. The feature uses a background thread to poll for changes, maintains proper synchronization with mutex locks, and provides visual indication of data modifications as they happen.

### Server Commands
The server also has an interactive interface with these commands:

- **list**: List all loaded files with their metadata
- **stats [filename]**: Show statistics for all files or a specific file
- **exit**: Shutdown the server
- **help**: Display help information

### Client Enhancements
- Real-time display functionality with live updates
- Connection health checking
- Improved error handling and retry logic

## 4. Dependencies

To build and run this project, you need:

- **C++ Compiler:** A modern C++ compiler supporting C++11 or later (e.g., GCC, Clang).
- **CMake:** Version 3.10 or higher for building the project.
- **gRPC:** The gRPC library and its dependencies (including Protocol Buffers). Installation instructions can be found at [grpc.io](https://grpc.io/docs/languages/cpp/quickstart/).
- **Protocol Buffers:** Version 3.x or higher (usually installed as part of gRPC).

*Note: The specific versions required might depend on your system setup. Ensure compatibility between gRPC and Protobuf.* 

## 5. Directory Structure

```
csv_handler/
├── build/          # Build directory (created by CMake)
├── client/
│   ├── csv_client.cpp     # Client logic implementation
│   ├── csv_client.hpp     # Client class header
│   ├── main.cpp           # Client executable entry point
│   ├── menu.cpp           # Client menu system implementation
│   └── menu.hpp           # Client menu system header
├── core/           # Core interfaces and data structures
│   ├── IStateMachine.hpp  # State machine interface
│   ├── Mutation.hpp       # Data mutation operations
│   ├── TableView.cpp      # Implementation of read-only view
│   └── TableView.hpp      # Read-only view abstraction
├── data/           # Directory containing sample CSV files
│   ├── mock_data.csv      # Sample CSV file with 3 columns
│   ├── mock_data1.csv     # Sample CSV file with 4 columns
│   └── test_data.csv      # Sample CSV file matching examples in this README
├── distributed/    # Distributed system components
│   ├── raft_node.cpp      # Raft consensus implementation
│   └── raft_node.hpp      # Raft node interface
├── persistence/    # Data persistence components
│   ├── DurableStateMachine.cpp  # Persistent state machine implementation
│   ├── DurableStateMachine.hpp  # Durable state machine interface
│   ├── Snapshot.cpp             # State snapshot implementation
│   ├── Snapshot.hpp             # Snapshot interface
│   ├── WriteAheadLog.cpp        # Write-ahead log implementation
│   └── WriteAheadLog.hpp        # WAL interface
├── proto/          # Protocol Buffers definitions
│   ├── cluster_service.proto    # Cluster communication service definition
│   ├── csv_service.proto        # Main service definition
│   ├── mutation.proto           # Mutation message definitions
│   ├── csv_service.grpc.pb.cc   # Generated gRPC C++ source
│   ├── csv_service.grpc.pb.h    # Generated gRPC C++ header
│   ├── csv_service.pb.cc        # Generated Protobuf C++ source
│   ├── csv_service.pb.h         # Generated Protobuf C++ header
│   └── generated/               # Directory for generated code
├── server/         # Server implementation
│   ├── main.cpp                 # Server executable entry point
│   ├── menu.cpp                 # Server menu system implementation
│   ├── menu.hpp                 # Server menu system header
│   └── network/                 # Network-related components
│       ├── csv_service_impl.cpp # Server RPC implementation
│       ├── csv_service_impl.hpp # Server RPC class header
│       ├── server_registry.cpp  # Server registry implementation
│       └── server_registry.hpp  # Server registry interface
├── storage/        # Storage implementations
│   ├── InMemoryStateMachine.cpp  # In-memory state machine implementation
│   ├── InMemoryStateMachine.hpp  # In-memory state machine header
│   ├── column_store.cpp          # Column store implementation
│   ├── column_store.hpp          # Column store interface
│   ├── csv_parser.cpp            # CSV parsing utilities
│   └── csv_parser.hpp            # CSV parser interface
├── utils/          # Utility functions
│   ├── file_utils.cpp            # File reading utilities
│   └── file_utils.hpp            # Header for file utilities
├── tests/          # Test suite
│   ├── test_client_server.cpp    # Client-server communication tests
│   ├── test_columnstore_operations.cpp # Columnstore operations tests
│   ├── test_data_mutation.cpp    # Data mutation tests
│   ├── test_persistence.cpp      # Persistence tests
│   ├── test_replication_and_failover.cpp # Replication and failover tests
│   └── test_invalid_cases.cpp    # Invalid cases tests
├── CMakeLists.txt               # CMake build configuration
└── README.md                    # This file
```

### Key Components:

- **`client/`**: Contains the gRPC client implementation, command handlers, and menu system.
  - `csv_client.cpp/hpp`: Core client functionality for communicating with the server.
  - `menu.cpp/hpp`: Command dispatch system for the client interface.
  - `main.cpp`: Entry point that initializes the menu system and handles CLI arguments.

- **`server/`**: Contains the gRPC server implementation, including the service logic.
  - `network/csv_service_impl.cpp/hpp`: Implementation of the gRPC service.
  - `menu.cpp/hpp`: Command dispatch system for the server interface.
  - `main.cpp`: Server entry point that starts the gRPC server and menu system.

- **`storage/`**: Core data storage and manipulation logic.
  - `column_store.cpp/hpp`: Defines the column-oriented data structure and operations.
  - `csv_parser.cpp/hpp`: Functionality for parsing CSV data into column format.

- **`proto/`**: Protocol Buffers definitions and generated code.
  - `csv_service.proto`: Defines all RPC methods and message types.
  - Generated files: Compiled protocol buffer code for C++.

- **`utils/`**: Helper utilities used by both client and server.

## 6. Build Instructions

1.  **Generate Protocol Buffer Files:**
    *   Navigate to the project root directory (`csv_handler/`).
    *   Ensure `protoc` and `grpc_cpp_plugin` are in your PATH or provide full paths.
    *   Run the following command:
        ```bash
        protoc --proto_path=. --cpp_out=./proto --grpc_out=./proto --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) proto/csv_service.proto
        ```
    *   This will generate the `.pb.h`, `.pb.cc`, `.grpc.pb.h`, and `.grpc.pb.cc` files inside the `proto/` directory.

2.  **Configure with CMake:**
    *   Create a build directory and navigate into it:
        ```bash
        mkdir -p build
        cd build
        ```
    *   Run CMake to configure the project:
        ```bash
        cmake ..
        ```

3.  **Compile:**
    *   From the `build` directory, run `make`:
        ```bash
        make
        ```
    *   This will create the `server` and `client` executables in the `build` directory.

4.  **Clean Build (if needed):**
    *   To clean the build and start fresh:
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

5.  **Rebuild After Changes:**
    *   After making changes to source files, simply run:
        ```bash
        # From the build directory
        make
        ```
    *   If you've added new source files or changed the CMakeLists.txt:
        ```bash
        # From the build directory
        cmake ..
        make
        ```

## 7. Running the Application

1.  **Start the Server:**
    *   From the `build` directory, run:
        ```bash
        ./server
        ```
    *   The server will start listening on `0.0.0.0:50051` (all network interfaces) and show its command menu.
    *   You can use server commands like `list` to see loaded files or `stats` to view statistics.
    *   Use the new `ip` command to display the server's IP address for remote client connections.

2.  **Run the Client:**
    *   The client can be run in two modes from the `build` directory.
    *   **Command-Line Mode:**
        *   Execute specific commands directly:
            ```bash
            ./client <server_address> <command> [arguments]
            ```
            Example: `./client localhost:50051 list`
    *   **Interactive Mode:**
        *   Start an interactive session:
            ```bash
            ./client <server_address>
            ```
            Example: `./client localhost:50051`

## 8. Distributed Usage and Deployment

The system supports a distributed architecture with multiple servers forming a cluster for fault tolerance and high availability. Data is automatically replicated across the cluster using a simplified Raft-inspired consensus algorithm.

### Cluster Setup

*   Start multiple server instances, each with a unique address and knowledge of its peers:
    ```bash
    # Start the first server (will become leader by default)
    ./server localhost:50051 localhost:50052 localhost:50053
    
    # Start the second server in another terminal
    ./server localhost:50052 localhost:50051 localhost:50053
    
    # Start the third server in another terminal
    ./server localhost:50053 localhost:50051 localhost:50052
    ```
*   Each server needs its own address as the first argument, followed by the addresses of all other servers in the cluster.
*   The servers will automatically establish connections with each other and elect a leader (by default, the server with the lexicographically smallest address).

### Client Connection to the Cluster

*   Connect a client to any server in the cluster:
    ```bash
    ./client localhost:50051
    ```
*   Clients can connect to any server in the cluster. Non-leader servers will automatically forward requests to the current leader.
*   For command-line mode, specify the command after the server address:
    ```bash
    ./client localhost:50051 upload test_data.csv
    ```

### Fault Tolerance and Leader Election

*   If the leader server fails, the remaining servers will automatically detect the failure and elect a new leader.
*   Clients connected to the failed leader will need to reconnect to another server.
*   Data uploaded before the leader failure is preserved and accessible through the new leader.

### Persistence and Recovery

*   The system now implements disk persistence, ensuring data survives server restarts.
*   All mutations (inserts, deletes) are logged to a Write-Ahead Log (WAL) before being applied.
*   Periodic snapshots are created to optimize recovery time.
*   On server startup, the system automatically recovers its state from disk (snapshots and WAL).
*   The persistence mechanism operates as a "sidecar" process alongside the in-memory state machine.
*   No special commands are needed - persistence happens automatically in the background.

### Data Replication

*   When a client uploads a file or makes changes to data on the leader, those changes are automatically replicated to all follower servers.
*   Changes are also persisted to disk on each server, ensuring durability even in case of complete cluster failure.
*   This ensures that if the leader fails, the new leader will have the most up-to-date data.
*   The replication and persistence processes are transparent to clients.

### Network Considerations

*   All servers must be able to communicate with each other directly.
*   For a production deployment across different networks, you may need to configure appropriate firewall rules and network routing.
*   For security in production environments, consider implementing authentication and encryption.

## 9. Example CSV Format and Operations

The system works with standard CSV format, primarily designed for numerical data:

```csv
ID,Value1,Value2
1,30,50000
2,25,60000
3,35,70000
```

### Data Operations:

1. **Upload:** Send a CSV file to the server:
   ```
   > upload ../data/test_data.csv
   ```

2. **View:** Display the contents of a stored file in a formatted table:
   ```
   > view test_data.csv
   ```
   Output:
   ```
   +------+----------+----------+
   |  ID  |  Value1  |  Value2  |
   +------+----------+----------+
   |  1   |  30      |  50000   |
   |  2   |  25      |  60000   |
   |  3   |  35      |  70000   |
   +------+----------+----------+
   ```

3. **Analyze:** Perform calculations on columns:
   ```
   > sum test_data.csv Value2
   Sum of column 'Value2' in file 'test_data.csv': 180000
   
   > avg test_data.csv Value1
   Average of column 'Value1' in file 'test_data.csv': 30
   ```

4. **Modify:** Insert or delete rows:
   ```
   > insert test_data.csv 4,40,80000
   > delete test_data.csv 2
   ```

## 10. Column Store Structure

Data is stored in the `ColumnStore` structure defined in `storage/column_store.hpp`:

```cpp
struct ColumnStore {
    std::vector<std::string> column_names;
    std::unordered_map<std::string, std::vector<std::string>> columns;
};
```

This structure allows efficient access to specific columns for operations like sum and average.

### Column Operations

The system provides these operations on the column store:
- `compute_sum`: Calculate the sum of numeric values in a column
- `compute_average`: Calculate the average of numeric values in a column
- `insert_row`: Add a new row to the store
- `delete_row`: Remove a row from the store

## 11. Test Suite

The project includes a comprehensive test suite that validates the functionality of the distributed columnstore database system. The tests are located in the `csv_handler/tests/` directory and cover various aspects of the system's functionality.

### Test Categories

1. **Client-Server Communication Tests** (`test_client_server.cpp`):
   - Tests basic client-server connectivity
   - Validates file upload functionality
   - Tests column operations through the client interface
   - Verifies data mutation operations
   - Tests multi-server connection scenarios

2. **Columnstore Operations Tests** (`test_columnstore_operations.cpp`):
   - Tests sum operations on numeric columns
   - Tests average operations on numeric columns
   - Validates error handling for operations on non-numeric columns
   - Tests operations on non-existent columns

3. **Data Mutation Tests** (`test_data_mutation.cpp`):
   - Tests row insertion functionality
   - Validates error handling for invalid data insertions
   - Tests row deletion functionality
   - Validates error handling for invalid row deletion operations

4. **Persistence Tests** (`test_persistence.cpp`):
   - Tests basic persistence of data across server restarts
   - Validates persistence of data mutations
   - Tests server restart behavior

5. **Replication and Failover Tests** (`test_replication_and_failover.cpp`):
   - Tests basic data replication across servers
   - Validates leader election after leader failure
   - Tests system behavior during follower server failures

6. **Invalid Cases Tests** (`test_invalid_cases.cpp`):
   - Tests error handling for connection to non-existent servers
   - Validates error handling for operations on non-existent files
   - Tests error handling for invalid column operations
   - Validates error handling for invalid row operations

### Running the Tests

To build and run the tests:

```bash
# Navigate to the tests directory
cd csv_handler/tests

# Create a build directory
mkdir -p build
cd build

# Configure with CMake
cmake ..

# Build the tests
make

# Run individual tests
./test_client_server
./test_columnstore_operations
./test_data_mutation
./test_persistence
./test_replication_and_failover
./test_invalid_cases

# Or run all tests in sequence
./test_client_server && ./test_columnstore_operations && ./test_data_mutation && ./test_persistence && ./test_replication_and_failover && ./test_invalid_cases
```

The test suite ensures that the system functions correctly and handles error cases appropriately, providing a safety net for future development and refactoring.
