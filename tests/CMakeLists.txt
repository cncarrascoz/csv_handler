# Tests CMakeLists.txt
include(GoogleTest)

# Add include directories for tests
include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  /opt/homebrew/Cellar/grpc/1.71.0/include  # Explicit gRPC include path 
)

# Common test utilities
add_library(test_common STATIC 
  common/test_utils.cpp
)
target_link_libraries(test_common PUBLIC 
  gtest 
  gmock
  csv_handler_cc_proto
  gRPC::grpc++
  gRPC::grpc
  protobuf::libprotobuf
)

# Unit tests
# Storage tests
add_executable(storage_tests
  storage/csv_parser_test.cpp
  storage/column_store_test.cpp
)
target_link_libraries(storage_tests
  storage
  gtest_main
  gmock_main
  test_common
)
gtest_discover_tests(storage_tests)

# Utils tests
add_executable(utils_tests
  utils/file_utils_test.cpp
)
target_link_libraries(utils_tests
  utils
  gtest_main
  gmock_main
  test_common
)
gtest_discover_tests(utils_tests)

# Integration tests
add_executable(integration_tests
  integration/server_client_test.cpp
)
target_link_libraries(integration_tests
  csv_handler_cc_proto
  storage
  utils
  gtest_main
  gmock_main
  test_common
  gRPC::grpc++
  gRPC::grpc
  protobuf::libprotobuf
)
gtest_discover_tests(integration_tests)

# CLI tests
add_executable(cli_tests
  cli/menu_dispatch_test.cpp
)
target_link_libraries(cli_tests
  csv_handler_cc_proto
  storage
  utils
  gtest_main
  gmock_main
  test_common
  gRPC::grpc++
  gRPC::grpc
  protobuf::libprotobuf
)
gtest_discover_tests(cli_tests)

# Main test target that runs all tests
add_custom_target(run_all_tests
  COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
  DEPENDS storage_tests utils_tests integration_tests cli_tests)
