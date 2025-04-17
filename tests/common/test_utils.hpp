#pragma once

#include <string>
#include <vector>
#include <memory>

#include "storage/column_store.hpp"

namespace csv_test {

/**
 * Utility class to create and manage test CSV files
 */
class TestDataGenerator {
public:
    // Generate a CSV string with the given number of rows and columns
    static std::string generateCsvString(int rows, int cols, bool includeHeader = true);
    
    // Generate a CSV file with the given number of rows and columns
    static std::string generateCsvFile(const std::string& filename, int rows, int cols, bool includeHeader = true);
    
    // Create a test CSV file with known content for testing
    static std::string createSimpleTestCsv(const std::string& filename);

    // Create a test CSV file with edge cases (empty cells, quoted values, etc.)
    static std::string createEdgeCaseTestCsv(const std::string& filename);
    
    // Create a very large CSV file for performance testing
    static std::string createLargeTestCsv(const std::string& filename, int rows);

    // Create a UTF-8 test CSV file
    static std::string createUtf8TestCsv(const std::string& filename);

    // Create a column store from a CSV string
    static ColumnStore createColumnStoreFromCsv(const std::string& csvString);
};

/**
 * Process launcher to test CLI interactions
 * Creates a process and communicates with it via pipes
 */
class ProcessRunner {
public:
    // Start a process with the given command
    ProcessRunner(const std::string& command);
    ~ProcessRunner();

    // Write to the process's stdin
    bool writeToProcess(const std::string& input);

    // Read from the process's stdout (up to timeout_ms)
    std::string readFromProcess(int timeout_ms = 1000);

    // Check if the process is still running
    bool isRunning();

    // Wait for the process to exit and return the exit code
    int waitForExit(int timeout_ms = 5000);

private:
    FILE* process_stdin_;
    FILE* process_stdout_;
    int pid_;
};

} // namespace csv_test
