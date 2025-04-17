#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "tests/common/test_utils.hpp"

#include <string>
#include <memory>
#include <regex>
#include <chrono>
#include <thread>

using namespace testing;
using namespace csv_test;

class ClientMenuTest : public ::testing::Test {
protected:
    std::string client_path;
    std::string server_path;
    std::unique_ptr<ProcessRunner> server_process;
    std::string data_dir = "tests/data/";
    
    void SetUp() override {
        // Use paths relative to the build directory
        client_path = "./client";
        server_path = "./server";
        
        // Create test data directory if it doesn't exist
        system("mkdir -p tests/data");
        
        // Create test CSV files
        TestDataGenerator::createSimpleTestCsv(data_dir + "test_cli.csv");
        
        // Start the server
        server_process = std::make_unique<ProcessRunner>(server_path);
        
        // Wait for server to start
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    
    void TearDown() override {
        // Stop the server by sending "exit" command
        if (server_process) {
            server_process->writeToProcess("exit");
            server_process.reset();
        }
    }
    
    // Run a client command and return the output
    std::string runClientCommand(const std::string& command) {
        std::string full_command = client_path + " localhost:50051 " + command;
        ProcessRunner process(full_command);
        
        // Wait for command to complete
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        // Read output
        return process.readFromProcess(1000);
    }
    
    // Run interactive client with multiple commands
    std::string runInteractiveClient(const std::vector<std::string>& commands) {
        ProcessRunner process(client_path + " localhost:50051");
        
        // Wait for client to start
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        std::string output = process.readFromProcess(1000);
        
        // Send each command
        for (const auto& cmd : commands) {
            process.writeToProcess(cmd);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            output += process.readFromProcess(1000);
        }
        
        return output;
    }
};

// Test the help command
TEST_F(ClientMenuTest, HelpCommand) {
    std::string output = runClientCommand("help");
    
    // Help output should contain command descriptions
    EXPECT_THAT(output, HasSubstr("Client commands:"));
    EXPECT_THAT(output, HasSubstr("upload"));
    EXPECT_THAT(output, HasSubstr("list"));
    EXPECT_THAT(output, HasSubstr("view"));
    EXPECT_THAT(output, HasSubstr("sum"));
    EXPECT_THAT(output, HasSubstr("avg"));
    EXPECT_THAT(output, HasSubstr("insert"));
    EXPECT_THAT(output, HasSubstr("delete"));
    EXPECT_THAT(output, HasSubstr("exit"));
}

// Test the list command when no files exist
TEST_F(ClientMenuTest, ListCommandNoFiles) {
    std::string output = runClientCommand("list");
    
    // Should indicate no files are loaded
    EXPECT_THAT(output, HasSubstr("Loaded files on server:"));
}

// Test uploading a file and then listing files
TEST_F(ClientMenuTest, UploadAndListCommands) {
    std::vector<std::string> commands = {
        "upload " + data_dir + "test_cli.csv",
        "list"
    };
    
    std::string output = runInteractiveClient(commands);
    
    // Should show upload success and then list the file
    EXPECT_THAT(output, HasSubstr("Upload successful"));
    EXPECT_THAT(output, HasSubstr(data_dir + "test_cli.csv"));
}

// Test upload, view, and sum commands in sequence
TEST_F(ClientMenuTest, UploadViewSumCommands) {
    std::vector<std::string> commands = {
        "upload " + data_dir + "test_cli.csv",
        "view " + data_dir + "test_cli.csv",
        "sum " + data_dir + "test_cli.csv Value1"
    };
    
    std::string output = runInteractiveClient(commands);
    
    // Check for success messages and output
    EXPECT_THAT(output, HasSubstr("Upload successful"));
    // View should show a formatted table
    EXPECT_THAT(output, HasSubstr("+"));
    EXPECT_THAT(output, HasSubstr("|"));
    // Sum should show the correct value
    EXPECT_THAT(output, HasSubstr("Sum of column 'Value1' in file"));
    EXPECT_THAT(output, HasSubstr("60"));  // Sum of 10+20+30
}

// Test the avg command
TEST_F(ClientMenuTest, AvgCommand) {
    std::vector<std::string> commands = {
        "upload " + data_dir + "test_cli.csv",
        "avg " + data_dir + "test_cli.csv Value2"
    };
    
    std::string output = runInteractiveClient(commands);
    
    // Check for average calculation
    EXPECT_THAT(output, HasSubstr("Average of column 'Value2' in file"));
    EXPECT_THAT(output, HasSubstr("200"));  // Avg of (100+200+300)/3
}

// Test insert and delete commands
TEST_F(ClientMenuTest, InsertAndDeleteCommands) {
    std::vector<std::string> commands = {
        "upload " + data_dir + "test_cli.csv",
        "insert " + data_dir + "test_cli.csv 4,40,400",
        "view " + data_dir + "test_cli.csv",
        "delete " + data_dir + "test_cli.csv 1",  // Delete the second row
        "view " + data_dir + "test_cli.csv"
    };
    
    std::string output = runInteractiveClient(commands);
    
    // Check for successful insert and delete
    EXPECT_THAT(output, HasSubstr("Row inserted successfully"));
    EXPECT_THAT(output, HasSubstr("Row deleted successfully"));
    
    // After all operations, should be able to see the updated data
    // First view should have 4 rows
    // Second view should have 3 rows (after deletion)
    
    // Count the number of row delimiters in the table
    // This is a bit hacky but effective for CLI testing
    std::regex row_regex("\\|.*\\|.*\\|.*\\|");
    std::sregex_iterator begin(output.begin(), output.end(), row_regex);
    std::sregex_iterator end;
    int row_count = std::distance(begin, end);
    
    // Should have at least 7 rows (headers + data rows across two views)
    EXPECT_GE(row_count, 7);
}

// Test error handling for non-existent files
TEST_F(ClientMenuTest, NonExistentFileError) {
    std::vector<std::string> commands = {
        "view nonexistent.csv",
        "sum nonexistent.csv Value1"
    };
    
    std::string output = runInteractiveClient(commands);
    
    // Should show error messages
    EXPECT_THAT(output, HasSubstr("Failed to view file"));
    EXPECT_THAT(output, HasSubstr("Failed to compute sum"));
}

// Test error handling for invalid commands
TEST_F(ClientMenuTest, InvalidCommandError) {
    std::string output = runClientCommand("invalid_command");
    
    // Should show error message
    EXPECT_THAT(output, HasSubstr("Unknown command"));
}

// Test error handling for commands with wrong arguments
TEST_F(ClientMenuTest, WrongArgumentsError) {
    std::vector<std::string> commands = {
        "upload",  // Missing filename
        "sum " + data_dir + "test_cli.csv"  // Missing column name
    };
    
    std::string output = runInteractiveClient(commands);
    
    // Should show error messages
    EXPECT_THAT(output, AnyOf(HasSubstr("Invalid arguments"), 
                              HasSubstr("Usage:"), 
                              HasSubstr("requires")));
}

class ServerMenuTest : public ::testing::Test {
protected:
    std::unique_ptr<ProcessRunner> server_process;
    
    void SetUp() override {
        // Start the server with an interactive process
        server_process = std::make_unique<ProcessRunner>("./server");
        
        // Wait for server to start
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    
    void TearDown() override {
        // Stop the server
        if (server_process) {
            server_process->writeToProcess("exit");
            server_process.reset();
        }
    }
    
    // Send a command to the server and get output
    std::string sendServerCommand(const std::string& command) {
        if (!server_process) {
            return "Server not running";
        }
        
        // Get any pending output
        std::string output = server_process->readFromProcess(100);
        
        // Send command
        server_process->writeToProcess(command);
        
        // Wait for command to process
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        // Get new output
        output += server_process->readFromProcess(1000);
        return output;
    }
};

// Test the server help command
TEST_F(ServerMenuTest, HelpCommand) {
    std::string output = sendServerCommand("help");
    
    // Help output should contain server command descriptions
    EXPECT_THAT(output, HasSubstr("Server commands:"));
    EXPECT_THAT(output, HasSubstr("exit"));
    EXPECT_THAT(output, HasSubstr("help"));
    EXPECT_THAT(output, HasSubstr("list"));
}

// Test the server list command when no files exist
TEST_F(ServerMenuTest, ListCommandNoFiles) {
    std::string output = sendServerCommand("list");
    
    // Should indicate no files are loaded
    EXPECT_THAT(output, HasSubstr("Loaded files:"));
    EXPECT_THAT(output, AnyOf(HasSubstr("None"), HasSubstr("(None)")));
}

// Test the server stats command
TEST_F(ServerMenuTest, StatsCommand) {
    std::string output = sendServerCommand("stats");
    
    // Should show server statistics
    EXPECT_THAT(output, HasSubstr("Statistics"));
}

// Test an invalid server command
TEST_F(ServerMenuTest, InvalidCommandError) {
    std::string output = sendServerCommand("invalid_command");
    
    // Should show error message
    EXPECT_THAT(output, HasSubstr("Unknown command"));
}

// Future extension test (disabled)
TEST(MenuFutureTest, DISABLED_DistributedCommands) {
    // TODO: Test distributed commands when implemented
    GTEST_SKIP() << "Distributed command tests not implemented yet";
}
