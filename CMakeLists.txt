cmake_minimum_required(VERSION 3.10)
project(CSV_Handler)

# Set C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable testing with CTest
enable_testing()

# Setup options for coverage
option(ENABLE_COVERAGE "Enable code coverage" OFF)
if(ENABLE_COVERAGE)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
  endif()
endif()

# Find packages using CONFIG mode (still need libs)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Store the gRPC include directories more explicitly
get_target_property(GRPC_INCLUDE_DIRS gRPC::grpc++ INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(PROTOBUF_INCLUDE_DIRS protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)

# If the target properties don't work, try to find them manually
if(NOT GRPC_INCLUDE_DIRS)
  # Try to find gRPC headers on standard paths
  find_path(GRPC_INCLUDE_DIRS grpc/grpc.h)
  find_path(GRPCPP_INCLUDE_DIRS grpcpp/grpcpp.h)
  
  if(GRPC_INCLUDE_DIRS AND GRPCPP_INCLUDE_DIRS)
    message(STATUS "Found gRPC includes: ${GRPC_INCLUDE_DIRS} and ${GRPCPP_INCLUDE_DIRS}")
  else()
    message(WARNING "Could not find gRPC includes, some headers may not be found!")
  endif()
endif()

# Include FetchContent
include(FetchContent)

# Fetch GoogleTest
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

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
    ${GRPC_INCLUDE_DIRS}
    ${GRPCPP_INCLUDE_DIRS}
    ${PROTOBUF_INCLUDE_DIRS}
)

# Create static libraries for components to be used in both production and tests
add_library(storage STATIC
    storage/csv_parser.cpp
    storage/column_store.cpp
)

add_library(utils STATIC
    utils/file_utils.cpp
)

target_link_libraries(storage PUBLIC csv_handler_cc_proto)
target_link_libraries(utils PUBLIC csv_handler_cc_proto)

# Server executable
add_executable(server
    server/main.cpp
    server/network/csv_service_impl.cpp
    server/menu.cpp
)

target_link_libraries(server
    csv_handler_cc_proto
    storage
    utils
    gRPC::grpc++
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

# Add a custom target for coverage reports
if(ENABLE_COVERAGE)
  add_custom_target(coverage
    COMMAND gcovr --root ${CMAKE_SOURCE_DIR} --exclude ".*tests/.*" --exclude ".*build/.*" --exclude ".*_deps/.*" --html --html-details -o ${CMAKE_BINARY_DIR}/coverage.html
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Generating coverage report..."
  )
endif()

# Add subdirectory for tests
add_subdirectory(tests)