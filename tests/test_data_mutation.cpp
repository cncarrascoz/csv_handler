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
#include <sstream>
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

// Helper function to check if output contains a specific string
bool outputContainsString(const std::string& output, const std::string& str) {
    return output.find(str) != std::string::npos;
}

// Global variables for test setup/teardown
pid_t server_pid;
std::string server_address = "localhost:50051";
std::string test_csv_path;

// Setup function for data mutation tests
void setupDataMutationTest() {
    // Create a test CSV file
    test_csv_path = createTestCsvFile("test_mutation.csv", 
        "ID,Name,Value\n1,Row1,100\n2,Row2,200\n3,Row3,300\n");
    
    // Start the server
    server_pid = startServer(server_address);
}

// Teardown function for data mutation tests
void teardownDataMutationTest() {
    // Stop the server
    stopServer(server_pid);
    
    // Remove the test CSV file
    std::filesystem::remove(test_csv_path);
}

// Test inserting a row
TEST(InsertRow) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Upload the test CSV file
    ASSERT_TRUE(client.UploadCsv(test_csv_path));
    
    // Insert a new row
    ASSERT_TRUE(client.InsertRow("test_mutation.csv", {"4", "Row4", "400"}));
}

// Test inserting a row with invalid data
TEST(InsertRowInvalidData) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Upload the test CSV file
    ASSERT_TRUE(client.UploadCsv(test_csv_path));
    
    // Try to insert a row with too few columns
    ASSERT_FALSE(client.InsertRow("test_mutation.csv", {"4", "Row4"}));
    
    // Try to insert a row with too many columns
    ASSERT_FALSE(client.InsertRow("test_mutation.csv", {"4", "Row4", "400", "Extra"}));
}

// Test deleting a row
TEST(DeleteRow) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Upload the test CSV file
    ASSERT_TRUE(client.UploadCsv(test_csv_path));
    
    // Delete a row
    client.DeleteRow("test_mutation.csv", 1); // Delete the second row (index 1)
}

// Test deleting a row with invalid index
TEST(DeleteRowInvalidIndex) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Upload the test CSV file
    ASSERT_TRUE(client.UploadCsv(test_csv_path));
    
    // Try to delete a row with a negative index
    client.DeleteRow("test_mutation.csv", -1);
    
    // Try to delete a row with an out-of-bounds index
    client.DeleteRow("test_mutation.csv", 100);
}

// Test operations on a non-existent file
TEST(OperationsOnNonExistentFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Try to insert a row into a non-existent file
    ASSERT_FALSE(client.InsertRow("non_existent.csv", {"1", "Row1", "100"}));
    
    // Try to delete a row from a non-existent file
    client.DeleteRow("non_existent.csv", 0);
}

int main() {
    // Redirect stderr to /dev/null to suppress error messages
    std::freopen("/dev/null", "w", stderr);
    
    std::cout << "Running data mutation tests...\n";
    
    setupDataMutationTest();
    
    RUN_TEST(InsertRow);
    RUN_TEST(InsertRowInvalidData);
    RUN_TEST(DeleteRow);
    RUN_TEST(DeleteRowInvalidIndex);
    RUN_TEST(OperationsOnNonExistentFile);
    
    teardownDataMutationTest();
    
    std::cout << "All data mutation tests passed!\n";
    return 0;
}
