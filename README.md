# gRPC CSV Handler

## 1. Project Overview

This project implements a client-server system in C++ using gRPC and Protocol Buffers. The primary goal is to allow a client application to send CSV data to a server, where the server parses the data and stores it efficiently in memory using a column-store format. The system provides a modular, extensible command interface for data manipulation and analysis, with clean separation between frontend and backend logic. It is designed with future distributed architecture in mind.

## 2. Architecture

- **Client-Server Model:** A standard client-server architecture where the client initiates requests and the server responds.
- **Command Dispatch System:** A flexible menu-based command system in both client and server that maps command strings to handler functions.
- **Communication:** gRPC is used for defining the service interface (`proto/csv_service.proto`) and handling remote procedure calls between the client and server.
- **Data Storage:** The server stores the parsed CSV data entirely in memory in a column-oriented format (map of column names to vectors of values), optimized for analytical queries.
- **Separation of Concerns:** Clear separation between interface, command processing, network communication, and storage logic.

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
- **exit**: Exit the program
- **help**: Display help information about available commands

### Server Commands
The server also has an interactive interface with these commands:

- **list**: List all loaded files with their metadata
- **stats [filename]**: Show statistics for all files or a specific file
- **exit**: Shutdown the server
- **help**: Display help information

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
│   ├── csv_client.cpp # Client logic implementation
│   ├── csv_client.hpp # Client class header
│   ├── main.cpp       # Client executable entry point
│   ├── menu.cpp       # Client menu system implementation
│   └── menu.hpp       # Client menu system header
├── data/           # Directory containing sample CSV files
│   ├── mock_data.csv  # Sample CSV file with 3 columns
│   ├── mock_data1.csv # Sample CSV file with 4 columns
│   └── test_data.csv  # Sample CSV file matching examples in this README
├── proto/
│   ├── csv_service.proto     # Service definition
│   ├── csv_service.pb.cc     # Generated Protobuf C++ source
│   ├── csv_service.pb.h      # Generated Protobuf C++ header
│   ├── csv_service.grpc.pb.cc # Generated gRPC C++ source
│   └── csv_service.grpc.pb.h # Generated gRPC C++ header
├── server/
│   ├── network/
│   │   ├── csv_service_impl.cpp # Server RPC implementation
│   │   └── csv_service_impl.hpp # Server RPC class header
│   ├── main.cpp                 # Server executable entry point
│   ├── menu.cpp                 # Server menu system implementation
│   └── menu.hpp                 # Server menu system header
├── storage/
│   ├── column_store.cpp # Column-oriented storage operations
│   ├── column_store.hpp # Column store data structure definition
│   ├── csv_parser.cpp   # Logic for parsing CSV data
│   └── csv_parser.hpp   # Header for CSV parser
├── utils/
│   ├── file_utils.cpp # File reading utilities
│   └── file_utils.hpp # Header for file utilities
├── CMakeLists.txt     # CMake build configuration
└── README.md          # This file
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
    *   The server will start listening on `localhost:50051` (or the configured address) and show its command menu.
    *   You can use server commands like `list` to see loaded files or `stats` to view statistics.

2.  **Run the Client:**
    *   The client can be run in two modes from the `build` directory.
    *   **Command-Line Mode:**
        *   Execute specific commands directly:
            ```bash
            ./client localhost:50051 upload ../data/test_data.csv
            ./client localhost:50051 list
            ./client localhost:50051 view test_data.csv
            ./client localhost:50051 sum test_data.csv Value2
            ```
    *   **Interactive Mode:**
        *   Start the client with only the server address:
            ```bash
            ./client localhost:50051
            ```
        *   You will be presented with a menu of available commands:
            ```
            Client commands:
            1. avg     - Calculate average of values in a column
            2. delete  - Delete a row from a file
            3. exit    - Exit the program
            4. help    - Display help information
            5. insert  - Insert a new row into a file
            6. list    - List all files on the server
            7. sum     - Calculate sum of values in a column
            8. upload  - Upload a CSV file to the server
            9. view    - View the contents of a file
            > 
            ```
        *   Type commands such as `upload ../data/test_data.csv`, `list`, or `view test_data.csv`.

## 8. Example CSV Format and Operations

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

## 9. Server Internals: Parsing and Storage

### Column Store Structure

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

## 10. Architecture and Design Patterns

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

This architecture facilitates the future addition of distributed features (e.g., automatic CSV reprinting when files change) with minimal refactoring.

## 11. Future Distributed Version

The current modular structure is designed to facilitate future expansion into a distributed system. Potential future components include:

- **Coordinator Node:** Manages cluster state, client requests, and distributes tasks to worker nodes.
- **Worker Nodes:** Store partitions of the data and execute parts of queries or processing tasks.
- **Replication:** Implement data replication across worker nodes for fault tolerance.
- **Heartbeat Mechanism:** Allow nodes to monitor each other's status.

The clean separation between the interface and backend logic will make these additions easier to implement.
