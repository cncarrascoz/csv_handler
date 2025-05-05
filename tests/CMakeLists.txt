cmake_minimum_required(VERSION 3.10)
project(csv_handler_tests)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/..
)

# Create a test client library
add_library(csv_client_test STATIC
    csv_client_test_impl.cpp
)

# Define test executables
set(TEST_SOURCES
    test_client_server.cpp
    test_columnstore_operations.cpp
    test_data_mutation.cpp
    test_persistence.cpp
    test_replication_and_failover.cpp
    test_invalid_cases.cpp
)

# Create test executables
foreach(test_source ${TEST_SOURCES})
    get_filename_component(test_name ${test_source} NAME_WE)
    add_executable(${test_name} ${test_source})
    
    # Link against our test client library
    target_link_libraries(${test_name}
        csv_client_test
    )
    
    # Add test to CTest
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

# Enable testing
enable_testing()

# Print message about running tests
message(STATUS "Tests configured. Run with 'ctest' or 'make test' after building.")
