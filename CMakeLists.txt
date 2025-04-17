cmake_minimum_required(VERSION 3.10)
project(CSV_Handler)

# Find packages using CONFIG mode (still need libs)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Define a static library target using the pre-generated files in source tree
add_library(csv_handler_cc_proto STATIC
    proto/csv_service.pb.cc
    proto/csv_service.grpc.pb.cc
    # Headers are not sources, but keep list for clarity if needed
    # proto/csv_service.pb.h
    # proto/csv_service.grpc.pb.h
)

# Link the generated code library against protobuf & grpc libs (Required for includes/flags)
target_link_libraries(csv_handler_cc_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

# Include directories
# Add proto/ subdir in source tree for generated headers
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/proto # Generated headers are here now
    ${CMAKE_CURRENT_SOURCE_DIR}/server
)

# Server executable
add_executable(server
    server/main.cpp
    server/network/csv_service_impl.cpp
    server/menu.cpp
    storage/csv_parser.cpp
    storage/column_store.cpp
    utils/file_utils.cpp
)

target_link_libraries(server
    csv_handler_cc_proto
    gRPC::grpc++
    gRPC::grpc
    protobuf::libprotobuf
)

# Client executable
add_executable(client
    client/main.cpp
    client/csv_client.cpp
    client/menu.cpp
    utils/file_utils.cpp
)

target_link_libraries(client
    csv_handler_cc_proto
    gRPC::grpc++
    gRPC::grpc
    protobuf::libprotobuf
)