#ifndef CSV_CLIENT_TEST_HPP
#define CSV_CLIENT_TEST_HPP

#include <string>
#include <vector>

// A simplified version of CsvClient for testing purposes
class CsvClient {
public:
    // Constructor with server addresses
    CsvClient(const std::vector<std::string>& server_addresses);
    
    // Destructor
    ~CsvClient();
    
    // Test connection to the server
    bool TestConnection();
    
    // Upload a CSV file to the server
    bool UploadCsv(const std::string& filepath);
    
    // List all CSV files on the server
    void ListFiles();
    
    // View the contents of a CSV file
    void ViewFile(const std::string& filename);
    
    // Compute the sum of a column in a CSV file
    void ComputeSum(const std::string& filename, const std::string& column);
    
    // Compute the average of a column in a CSV file
    void ComputeAverage(const std::string& filename, const std::string& column);
    
    // Insert a row into a CSV file
    bool InsertRow(const std::string& filename, const std::vector<std::string>& values);
    
    // Delete a row from a CSV file
    void DeleteRow(const std::string& filename, int row_index);

private:
    // Server addresses
    std::vector<std::string> server_addresses;
};

#endif // CSV_CLIENT_TEST_HPP
