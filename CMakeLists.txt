cmake_minimum_required(VERSION 3.10)
project(CSV_Handler)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find packages using CONFIG mode (still need libs)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Define a static library target using the pre-generated files in source tree
add_library(csv_handler_cc_proto STATIC
    proto/csv_service.pb.cc
    proto/csv_service.grpc.pb.cc
)

# Link the generated code libraries against protobuf & grpc libs
target_link_libraries(csv_handler_cc_proto PUBLIC 
    protobuf::libprotobuf 
    gRPC::grpc++
)

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    # ${CMAKE_CURRENT_SOURCE_DIR}/proto # Remove this - handled by target includes/linking
)

# Core library with state machine interface and table view
add_library(core STATIC
    core/TableView.cpp
)

# Storage library with in-memory state machine and CSV parsing
add_library(storage STATIC
    storage/csv_parser.cpp
    storage/column_store.cpp
    storage/InMemoryStateMachine.cpp
)

# Persistence library with WAL and snapshot functionality
add_library(persistence STATIC
    persistence/WriteAheadLog.cpp
    persistence/Snapshot.cpp
    persistence/DurableStateMachine.cpp
)

# Distributed library with fault tolerance implementation
add_library(distributed STATIC
    server/network/server_registry.cpp
)

# Utils library with file handling utilities
add_library(utils STATIC
    utils/file_utils.cpp
)

# Link dependencies
target_link_libraries(core PUBLIC csv_handler_cc_proto)
target_link_libraries(storage PUBLIC core)
target_link_libraries(persistence PUBLIC storage)
target_link_libraries(distributed PUBLIC persistence)

# Server executable
add_executable(server
    server/main.cpp
    server/network/csv_service_impl.cpp
    server/menu.cpp
)

target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(server
    csv_handler_cc_proto
    storage
    core
    utils
    persistence
    distributed
    gRPC::grpc++
    gRPC::grpc++_reflection
    gRPC::grpc
    protobuf::libprotobuf
)

# Client executable
add_executable(client
    client/main.cpp
    client/csv_client.cpp
    client/menu.cpp
)

target_link_libraries(client
    csv_handler_cc_proto
    utils
    gRPC::grpc++
    gRPC::grpc
    protobuf::libprotobuf
)