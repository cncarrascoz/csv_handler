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

// Setup function for client-server tests
void setupClientServerTest() {
    // Create a test CSV file
    test_csv_path = createTestCsvFile("test_client_server.csv", 
        "ID,Name,Value\n1,Row1,100\n2,Row2,200\n3,Row3,300\n");
    
    // Start the server
    server_pid = startServer(server_address);
}

// Teardown function for client-server tests
void teardownClientServerTest() {
    // Stop the server
    stopServer(server_pid);
    
    // Remove the test CSV file
    std::filesystem::remove(test_csv_path);
}

// Test client can connect to server
TEST(ClientCanConnectToServer) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
}

// Test client can upload a CSV file
TEST(ClientCanUploadCsvFile) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    ASSERT_TRUE(client.UploadCsv(test_csv_path));
}

// Test column operations
TEST(ColumnOperations) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Create a numeric test file
    std::string numeric_csv_path = createTestCsvFile("test_numeric.csv", 
        "ID,Value\n1,100\n2,200\n3,300\n");
    
    // Upload the file
    ASSERT_TRUE(client.UploadCsv(numeric_csv_path));
    
    // Test sum operation
    client.ComputeSum("test_numeric.csv", "Value");
    
    // Test average operation
    client.ComputeAverage("test_numeric.csv", "Value");
    
    // Clean up
    std::filesystem::remove(numeric_csv_path);
}

// Test data mutations
TEST(DataMutations) {
    CsvClient client({server_address});
    ASSERT_TRUE(client.TestConnection());
    
    // Create a test file
    std::string mutation_csv_path = createTestCsvFile("test_mutation.csv", 
        "ID,Name,Value\n1,Row1,100\n2,Row2,200\n3,Row3,300\n");
    
    // Upload the file
    ASSERT_TRUE(client.UploadCsv(mutation_csv_path));
    
    // Insert a row
    ASSERT_TRUE(client.InsertRow("test_mutation.csv", {"4", "Row4", "400"}));
    
    // Delete a row
    client.DeleteRow("test_mutation.csv", 1);
    
    // Clean up
    std::filesystem::remove(mutation_csv_path);
}

// Test multi-server connection
TEST(MultiServerConnection) {
    // Start a second server
    std::string second_server_address = "localhost:50052";
    pid_t second_server_pid = startServer(second_server_address);
    
    // Create a client with multiple server addresses
    std::vector<std::string> server_addresses = {server_address, second_server_address};
    CsvClient client(server_addresses);
    
    // Test connection
    ASSERT_TRUE(client.TestConnection());
    
    // Test operations
    ASSERT_TRUE(client.UploadCsv(test_csv_path));
    
    // Stop the second server
    stopServer(second_server_pid);
}

int main() {
    // Redirect stderr to /dev/null to suppress error messages
    std::freopen("/dev/null", "w", stderr);
    
    std::cout << "Running client-server tests...\n";
    
    setupClientServerTest();
    
    RUN_TEST(ClientCanConnectToServer);
    RUN_TEST(ClientCanUploadCsvFile);
    RUN_TEST(ColumnOperations);
    RUN_TEST(DataMutations);
    RUN_TEST(MultiServerConnection);
    
    teardownClientServerTest();
    
    std::cout << "All client-server tests passed!\n";
    return 0;
}
