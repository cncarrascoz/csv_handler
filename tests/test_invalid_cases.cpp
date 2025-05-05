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
std::string test_csv_path;

// Setup function for invalid cases tests
void setupInvalidCasesTest() {
    // Create a test CSV file
    test_csv_path = createTestCsvFile("test_invalid.csv", 
        "ID,Name,Value\n1,Row1,100\n2,Row2,200\n3,Row3,300\n");
    
    // Start the server
    server_pid = startServer(server_address);
}

// Teardown function for invalid cases tests
void teardownInvalidCasesTest() {
    // Stop the server
    stopServer(server_pid);
    
    // Remove the test CSV file
    std::filesystem::remove(test_csv_path);
}

// Test connecting to a non-existent server
TEST(ConnectToNonExistentServer) {
    // Create a client with a non-existent server address
    CsvClient client({"localhost:59999"});
    
    // The connection should fail
    ASSERT_FALSE(client.TestConnection());
}

// Test viewing a non-existent file
TEST(ViewNonExistentFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Try to view a non-existent file
    client.ViewFile("non_existent.csv");
}

// Test computing sum on a non-existent file
TEST(ComputeSumOnNonExistentFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Try to compute sum on a non-existent file
    client.ComputeSum("non_existent.csv", "Value");
}

// Test computing average on a non-existent file
TEST(ComputeAverageOnNonExistentFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Try to compute average on a non-existent file
    client.ComputeAverage("non_existent.csv", "Value");
}

// Test inserting a row into a non-existent file
TEST(InsertRowIntoNonExistentFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Try to insert a row into a non-existent file
    ASSERT_FALSE(client.InsertRow("non_existent.csv", {"4", "Row4", "400"}));
}

// Test deleting a row from a non-existent file
TEST(DeleteRowFromNonExistentFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Try to delete a row from a non-existent file
    client.DeleteRow("non_existent.csv", 0);
}

int main() {
    // Redirect stderr to /dev/null to suppress error messages
    std::freopen("/dev/null", "w", stderr);
    
    std::cout << "Running invalid cases tests...\n";
    
    // These tests don't need server setup
    RUN_TEST(ConnectToNonExistentServer);
    
    // These tests need server setup
    setupInvalidCasesTest();
    
    RUN_TEST(ViewNonExistentFile);
    RUN_TEST(ComputeSumOnNonExistentFile);
    RUN_TEST(ComputeAverageOnNonExistentFile);
    RUN_TEST(InsertRowIntoNonExistentFile);
    RUN_TEST(DeleteRowFromNonExistentFile);
    
    teardownInvalidCasesTest();
    
    std::cout << "All invalid cases tests passed!\n";
    return 0;
}
