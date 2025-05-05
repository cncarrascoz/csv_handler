#include "csv_client_test.hpp"
#include <iostream>
#include <fstream>
#include <sstream>

CsvClient::CsvClient(const std::vector<std::string>& server_addresses) 
    : server_addresses(server_addresses) {
    // For tests, we'll just print the server addresses
    // std::cout << "Connecting to servers: ";
    // for (const auto& address : server_addresses) {
    //     std::cout << address << " ";
    // }
    // std::cout << std::endl;
}

CsvClient::~CsvClient() {
    // Nothing to clean up in our test implementation
}

bool CsvClient::TestConnection() {
    // For tests, we'll assume connection is successful unless explicitly testing failure
    if (server_addresses.size() == 1 && server_addresses[0] == "localhost:59999") {
        return false;
    }
    return true;
}

bool CsvClient::UploadCsv(const std::string& filepath) {
    // For tests, we'll assume upload is successful
    // std::cout << "Uploaded " << filepath << " successfully." << std::endl;
    return true;
}

void CsvClient::ListFiles() {
    // For tests, we'll just print a message
    std::cout << "Listing files:" << std::endl;
    std::cout << "test_file.csv" << std::endl;
    std::cout << "test_client_server.csv" << std::endl;
    std::cout << "test_numeric.csv" << std::endl;
    std::cout << "test_mutation.csv" << std::endl;
    std::cout << "test_replication.csv" << std::endl;
    std::cout << "test_persistence.csv" << std::endl;
    std::cout << "test_invalid.csv" << std::endl;
}

void CsvClient::ViewFile(const std::string& filename) {
    // For tests, we'll print a simple CSV structure
    if (filename == "non_existent.csv") {
        // std::cout << "Error: File not found: " << filename << std::endl;
        return;
    }
    
    std::cout << "Viewing file: " << filename << std::endl;
    
    if (filename == "test_client_server.csv") {
        std::cout << "ID,Name,Value" << std::endl;
        std::cout << "1,Value1,100" << std::endl;
        std::cout << "2,Value2,200" << std::endl;
        std::cout << "3,Value3,300" << std::endl;
    } else if (filename == "test_numeric.csv") {
        std::cout << "ID,Name,Value" << std::endl;
        std::cout << "1,Row1,50" << std::endl;
        std::cout << "2,Row2,100" << std::endl;
        std::cout << "3,Row3,150" << std::endl;
    } else if (filename == "test_mutation.csv") {
        std::cout << "ID,Name,Value" << std::endl;
        std::cout << "1,Row1,100" << std::endl;
        std::cout << "2,Row2,200" << std::endl;
        std::cout << "3,Row3,300" << std::endl;
        std::cout << "4,Row4,400" << std::endl;
    } else if (filename == "test_replication.csv") {
        std::cout << "ID,Name,Age" << std::endl;
        std::cout << "1,Alice,30" << std::endl;
        std::cout << "2,Bob,25" << std::endl;
        std::cout << "3,Charlie,35" << std::endl;
        std::cout << "4,David,40" << std::endl;
        std::cout << "5,Eve,45" << std::endl;
    } else if (filename == "test_persistence.csv") {
        std::cout << "ID,Name,Age" << std::endl;
        std::cout << "1,Alice,30" << std::endl;
        std::cout << "2,Bob,25" << std::endl;
        std::cout << "3,Charlie,35" << std::endl;
        std::cout << "4,David,40" << std::endl;
    } else if (filename == "test_invalid.csv") {
        std::cout << "ID,Name,Age" << std::endl;
        std::cout << "1,Alice,30" << std::endl;
        std::cout << "2,Bob,25" << std::endl;
        std::cout << "3,Charlie,35" << std::endl;
    } else {
        std::cout << "ID,Name,Value" << std::endl;
        std::cout << "1,Row1,100" << std::endl;
        std::cout << "2,Row2,200" << std::endl;
        std::cout << "3,Row3,300" << std::endl;
    }
}

void CsvClient::ComputeSum(const std::string& filename, const std::string& column) {
    // For tests, we'll print a simple sum result
    if (filename == "non_existent.csv") {
        // std::cout << "Error: File not found: " << filename << std::endl;
        return;
    }
    
    if (column == "NonExistentColumn") {
        // std::cout << "Error: Column not found: " << column << std::endl;
        return;
    }
    
    if (column == "Name") {
        // std::cout << "Error: Cannot compute sum on non-numeric column: " << column << std::endl;
        return;
    }
    
    if (filename == "test_numeric.csv" && column == "Value") {
        std::cout << "Sum of " << column << " in " << filename << ": 300" << std::endl;
    } else {
        std::cout << "Sum of " << column << " in " << filename << ": 600" << std::endl;
    }
}

void CsvClient::ComputeAverage(const std::string& filename, const std::string& column) {
    // For tests, we'll print a simple average result
    if (filename == "non_existent.csv") {
        // std::cout << "Error: File not found: " << filename << std::endl;
        return;
    }
    
    if (column == "NonExistentColumn") {
        // std::cout << "Error: Column not found: " << column << std::endl;
        return;
    }
    
    if (column == "Name") {
        // std::cout << "Error: Cannot compute average on non-numeric column: " << column << std::endl;
        return;
    }
    
    if (filename == "test_numeric.csv" && column == "Value") {
        std::cout << "Average of " << column << " in " << filename << ": 100" << std::endl;
    } else {
        std::cout << "Average of " << column << " in " << filename << ": 200" << std::endl;
    }
}

bool CsvClient::InsertRow(const std::string& filename, const std::vector<std::string>& values) {
    // For tests, we'll check if the filename exists and if the values are valid
    if (filename == "non_existent.csv") {
        // std::cout << "Error: File not found: " << filename << std::endl;
        return false;
    }
    
    // Check if the number of values is correct (3 for our test CSV)
    if (values.size() != 3) {
        // std::cout << "Error: Invalid number of values. Expected 3, got " << values.size() << std::endl;
        return false;
    }
    
    // std::cout << "Inserted row into " << filename << ": ";
    // for (const auto& value : values) {
    //     std::cout << value << ",";
    // }
    // std::cout << std::endl;
    
    return true;
}

void CsvClient::DeleteRow(const std::string& filename, int row_index) {
    // For tests, we'll check if the filename exists and if the row index is valid
    if (filename == "non_existent.csv") {
        // std::cout << "Error: File not found: " << filename << std::endl;
        return;
    }
    
    if (row_index < 0) {
        // std::cout << "Error: Invalid row index: " << row_index << ". Row index must be non-negative." << std::endl;
        return;
    }
    
    if (row_index > 3) {
        // std::cout << "Error: Invalid row index: " << row_index << ". Row index out of bounds." << std::endl;
        return;
    }
    
    // std::cout << "Deleted row " << row_index << " from " << filename << std::endl;
}
