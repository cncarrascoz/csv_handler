#include "test_utils.hpp"
#include "utils/file_utils.hpp"
#include "storage/csv_parser.hpp"

#include <fstream>
#include <random>
#include <sstream>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdexcept>
#include <poll.h>
#include <fcntl.h>
#include <signal.h>  // For kill() and SIGTERM/SIGKILL

namespace csv_test {

//------------------------------------------------------------------------------
// TestDataGenerator Implementation
//------------------------------------------------------------------------------
std::string TestDataGenerator::generateCsvString(int rows, int cols, bool includeHeader) {
    std::stringstream ss;
    
    // Add header row if requested
    if (includeHeader) {
        for (int c = 0; c < cols; c++) {
            if (c > 0) ss << ",";
            ss << "Col" << c;
        }
        ss << "\n";
    }
    
    // Add data rows
    for (int r = 0; r < rows; r++) {
        for (int c = 0; c < cols; c++) {
            if (c > 0) ss << ",";
            ss << (r * cols + c);  // Generate simple numeric values
        }
        ss << "\n";
    }
    
    return ss.str();
}

std::string TestDataGenerator::generateCsvFile(const std::string& filename, int rows, int cols, bool includeHeader) {
    std::string csv_content = generateCsvString(rows, cols, includeHeader);
    
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    
    file << csv_content;
    file.close();
    
    return csv_content;
}

std::string TestDataGenerator::createSimpleTestCsv(const std::string& filename) {
    std::stringstream ss;
    
    // Simple CSV with numeric data
    ss << "ID,Value1,Value2\n";
    ss << "1,10,100\n";
    ss << "2,20,200\n";
    ss << "3,30,300\n";
    
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    
    file << ss.str();
    file.close();
    
    return ss.str();
}

std::string TestDataGenerator::createEdgeCaseTestCsv(const std::string& filename) {
    std::stringstream ss;
    
    // CSV with edge cases: empty cells, quoted values, quoted commas
    ss << "ID,Text,Value,Notes\n";
    ss << "1,\"Quoted text\",100,Normal\n";
    ss << "2,,200,Empty cell in column B\n";
    ss << "3,\"Text with, comma\",300,Quoted comma\n";
    ss << "4,\"Line\nbreak\",400,Quoted newline\n";
    ss << "5,\"\"\"Double quotes\"\"\",500,Escaped quotes\n";
    
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    
    file << ss.str();
    file.close();
    
    return ss.str();
}

std::string TestDataGenerator::createLargeTestCsv(const std::string& filename, int rows) {
    // For large files, write directly to file instead of creating string in memory
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    
    // Write header
    file << "ID,Value1,Value2,Value3\n";
    
    // Write rows
    for (int i = 0; i < rows; i++) {
        file << i << "," << (i * 10) << "," << (i * 100) << "," << (i * 1000) << "\n";
        
        // Flush every 10000 rows to avoid buffer issues
        if (i % 10000 == 0) {
            file.flush();
        }
    }
    
    file.close();
    
    return "Large CSV file with " + std::to_string(rows) + " rows created";
}

std::string TestDataGenerator::createUtf8TestCsv(const std::string& filename) {
    std::stringstream ss;
    
    // CSV with UTF-8 characters
    ss << "ID,Name,Description\n";
    ss << "1,José,Spanish name with accent\n";
    ss << "2,München,German city with umlaut\n";
    ss << "3,北京,Beijing in Chinese\n";
    ss << "4,こんにちは,Hello in Japanese\n";
    ss << "5,Café,French word with accent\n";
    
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    
    file << ss.str();
    file.close();
    
    return ss.str();
}

ColumnStore TestDataGenerator::createColumnStoreFromCsv(const std::string& csvString) {
    return csv::parse_csv(csvString);
}

//------------------------------------------------------------------------------
// ProcessRunner Implementation
//------------------------------------------------------------------------------
ProcessRunner::ProcessRunner(const std::string& command) 
    : process_stdin_(nullptr), process_stdout_(nullptr), pid_(-1) {
    
    int stdin_pipe[2];
    int stdout_pipe[2];
    
    if (pipe(stdin_pipe) < 0 || pipe(stdout_pipe) < 0) {
        throw std::runtime_error("Failed to create pipes");
    }
    
    pid_ = fork();
    if (pid_ < 0) {
        throw std::runtime_error("Failed to fork process");
    }
    
    if (pid_ == 0) {
        // Child process
        
        // Close unused pipe ends
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        
        // Redirect stdin and stdout
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        
        // Close duplicated pipe ends
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        
        // Execute the command
        execl("/bin/sh", "sh", "-c", command.c_str(), nullptr);
        
        // If we get here, execl failed
        exit(EXIT_FAILURE);
    }
    
    // Parent process
    
    // Close unused pipe ends
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);
    
    // Create FILE* for the pipe ends
    process_stdin_ = fdopen(stdin_pipe[1], "w");
    process_stdout_ = fdopen(stdout_pipe[0], "r");
    
    if (!process_stdin_ || !process_stdout_) {
        throw std::runtime_error("Failed to open process streams");
    }
    
    // Set stdout to non-blocking
    int flags = fcntl(fileno(process_stdout_), F_GETFL, 0);
    fcntl(fileno(process_stdout_), F_SETFL, flags | O_NONBLOCK);
}

ProcessRunner::~ProcessRunner() {
    if (process_stdin_) {
        fclose(process_stdin_);
    }
    
    if (process_stdout_) {
        fclose(process_stdout_);
    }
    
    if (pid_ > 0) {
        // Send SIGTERM to the process
        kill(pid_, SIGTERM);
        
        // Wait for the process to exit
        waitForExit(1000);
        
        // If the process is still running, send SIGKILL
        if (isRunning()) {
            kill(pid_, SIGKILL);
            waitForExit(1000);
        }
    }
}

bool ProcessRunner::writeToProcess(const std::string& input) {
    if (!process_stdin_) {
        return false;
    }
    
    if (fprintf(process_stdin_, "%s\n", input.c_str()) < 0) {
        return false;
    }
    
    fflush(process_stdin_);
    return true;
}

std::string ProcessRunner::readFromProcess(int timeout_ms) {
    if (!process_stdout_) {
        return "";
    }
    
    std::string output;
    char buffer[1024];
    
    struct pollfd fd;
    fd.fd = fileno(process_stdout_);
    fd.events = POLLIN;
    
    // Wait for data to be available
    int result = poll(&fd, 1, timeout_ms);
    if (result <= 0) {
        // Timeout or error
        return output;
    }
    
    // Read data
    while (true) {
        ssize_t bytesRead = read(fd.fd, buffer, sizeof(buffer) - 1);
        if (bytesRead <= 0) {
            break;
        }
        
        buffer[bytesRead] = '\0';
        output += buffer;
    }
    
    return output;
}

bool ProcessRunner::isRunning() {
    if (pid_ <= 0) {
        return false;
    }
    
    int status;
    pid_t result = waitpid(pid_, &status, WNOHANG);
    
    if (result == 0) {
        // Process is still running
        return true;
    }
    
    return false;
}

int ProcessRunner::waitForExit(int timeout_ms) {
    if (pid_ <= 0) {
        return -1;
    }
    
    int status;
    pid_t result;
    
    // Try to wait with timeout
    for (int i = 0; i < timeout_ms / 10; i++) {
        result = waitpid(pid_, &status, WNOHANG);
        if (result == pid_) {
            // Process has exited
            return WEXITSTATUS(status);
        }
        
        // Sleep for 10ms
        usleep(10 * 1000);
    }
    
    // Process didn't exit within timeout
    return -1;
}

} // namespace csv_test
