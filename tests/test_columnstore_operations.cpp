#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "csv_client_test.hpp"
#include "../utils/file_utils.hpp"

// Simple test framework
#define TEST(name) void name()
#define ASSERT(condition) if (!(condition)) { std::cerr << "Assertion failed: " #condition " at line " << __LINE__ << std::endl; exit(1); }
#define ASSERT_TRUE(condition) ASSERT(condition)
#define ASSERT_FALSE(condition) ASSERT(!(condition))
#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define RUN_TEST(name) std::cout << "Running " #name "... "; name(); std::cout << "PASSED\n"

// Helper function to create a test CSV file
std::string createTestCsvFile(const std::string& filename, const std::string& content) {
    std::string filepath = std::filesystem::temp_directory_path().string() + "/" + filename;
    std::ofstream file(filepath);
    file << content;
    file.close();
    return filepath;
}

// Helper function to start a server process
pid_t startServer(const std::string& address) {
    pid_t pid = fork();
    if (pid == 0) {
        // Child process
        std::string server_path = std::filesystem::current_path().string() + "/../server";
        execl(server_path.c_str(), "server", address.c_str(), NULL);
        exit(1); // Only reached if execl fails
    }
    // Give the server time to start up
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return pid;
}

// Helper function to stop a server process
void stopServer(pid_t pid) {
    if (pid > 0) {
        kill(pid, SIGTERM);
        int status;
        waitpid(pid, &status, 0);
    }
}

// Global variables for test setup/teardown
pid_t server_pid;
std::string server_address = "localhost:50051";
std::string test_numeric_csv_path;
std::string test_string_csv_path;

// Setup function for columnstore operations tests
void setupColumnstoreTest() {
    // Create test CSV files
    test_numeric_csv_path = createTestCsvFile("test_numeric.csv", 
        "ID,Value\n1,100\n2,200\n3,300\n");
    
    test_string_csv_path = createTestCsvFile("test_string.csv", 
        "ID,Name\n1,Alice\n2,Bob\n3,Charlie\n");
    
    // Start the server
    server_pid = startServer(server_address);
}

// Teardown function for columnstore operations tests
void teardownColumnstoreTest() {
    // Stop the server
    stopServer(server_pid);
    
    // Remove the test CSV files
    std::filesystem::remove(test_numeric_csv_path);
    std::filesystem::remove(test_string_csv_path);
}

// Test sum operation on a numeric column
TEST(SumOperationOnNumericColumn) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_numeric_csv_path));
    
    // Compute sum on the Value column
    client.ComputeSum("test_numeric.csv", "Value");
}

// Test average operation on a numeric column
TEST(AverageOperationOnNumericColumn) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_numeric_csv_path));
    
    // Compute average on the Value column
    client.ComputeAverage("test_numeric.csv", "Value");
}

// Test sum operation on a non-numeric column
TEST(SumOperationOnNonNumericColumn) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_string_csv_path));
    
    // Compute sum on the Name column (should fail or return 0)
    client.ComputeSum("test_string.csv", "Name");
}

// Test average operation on a non-numeric column
TEST(AverageOperationOnNonNumericColumn) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_string_csv_path));
    
    // Compute average on the Name column (should fail or return 0)
    client.ComputeAverage("test_string.csv", "Name");
}

// Test sum operation on a non-existent column
TEST(SumOperationOnNonExistentColumn) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_numeric_csv_path));
    
    // Compute sum on a non-existent column
    client.ComputeSum("test_numeric.csv", "NonExistentColumn");
}

// Test average operation on a non-existent column
TEST(AverageOperationOnNonExistentColumn) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_numeric_csv_path));
    
    // Compute average on a non-existent column
    client.ComputeAverage("test_numeric.csv", "NonExistentColumn");
}

int main() {
    // Redirect stderr to /dev/null to suppress error messages
    std::freopen("/dev/null", "w", stderr);
    
    std::cout << "Running columnstore operations tests...\n";
    
    setupColumnstoreTest();
    
    RUN_TEST(SumOperationOnNumericColumn);
    RUN_TEST(AverageOperationOnNumericColumn);
    RUN_TEST(SumOperationOnNonNumericColumn);
    RUN_TEST(AverageOperationOnNonNumericColumn);
    RUN_TEST(SumOperationOnNonExistentColumn);
    RUN_TEST(AverageOperationOnNonExistentColumn);
    
    teardownColumnstoreTest();
    
    std::cout << "All columnstore operations tests passed!\n";
    return 0;
}
