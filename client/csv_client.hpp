// client/csv_client.hpp: Defines the CsvClient class for interacting with the server.
#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "proto/csv_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using csvservice::CsvService;
using csvservice::CsvUploadRequest;
using csvservice::CsvUploadResponse;
using csvservice::Empty;
using csvservice::CsvFileList;
using csvservice::ViewFileRequest;
using csvservice::ViewFileResponse;
using csvservice::ColumnOperationRequest;
using csvservice::NumericResponse;
using csvservice::InsertRowRequest;
using csvservice::DeleteRowRequest;
using csvservice::ModificationResponse;

class CsvClient {
public:
    // Constructor with a single server address
    CsvClient(std::shared_ptr<Channel> channel);
    
    // Constructor with multiple server addresses for fault tolerance
    CsvClient(const std::vector<std::string>& server_addresses);
    
    ~CsvClient(); // Destructor to clean up display threads

    // Tests if the client can connect to the server
    // Returns true if connection is successful, false otherwise
    bool TestConnection();

    // Attempts to upload a CSV file to the server.
    // Returns true on success, false otherwise.
    bool UploadCsv(const std::string& filename);

    // Requests a list of loaded filenames from the server.
    // Prints the list to stdout.
    void ListFiles();
    
    // Retrieves and displays the content of a file stored on the server.
    void ViewFile(const std::string& filename);
    
    // Requests the server to compute the sum of values in a column.
    void ComputeSum(const std::string& filename, const std::string& column_name);
    
    // Requests the server to compute the average of values in a column.
    void ComputeAverage(const std::string& filename, const std::string& column_name);
    
    // Inserts a new row into a file on the server.
    // Returns true on success, false otherwise.
    bool InsertRow(const std::string& filename, const std::string& row_data);
    
    // Deletes a row from a file on the server.
    // Returns true on success, false otherwise.
    bool DeleteRow(const std::string& filename, int row_index);

    // Opens a new terminal window that displays the CSV file and updates in real-time
    void DisplayFile(const std::string& filename);
    
    // Stops the display thread for a specific file
    void StopDisplayThread(const std::string& filename);
    
    // Stops all display threads
    void StopAllDisplayThreads();

private:
    // Try to reconnect to another server if current connection fails
    bool TryReconnect();
    
    // Create a channel to a specific server address
    std::shared_ptr<Channel> CreateChannel(const std::string& server_address);
    
    std::unique_ptr<CsvService::Stub> stub_;
    std::vector<std::string> server_addresses_; // List of all available servers
    std::string current_server_address_; // Currently connected server
    int current_server_index_ = 0; // Index of current server in the list
    std::mutex reconnect_mutex_; // Mutex for thread-safe reconnection
    
    // Thread management for display command
    struct DisplayThreadInfo {
        std::thread thread;
        std::atomic<bool> running;
        std::string filename;
        std::string temp_filename; // Path to the temp file for display
        std::string last_content;
        std::mutex content_mutex;
    };
    
    std::mutex display_threads_mutex_;
    std::unordered_map<std::string, std::unique_ptr<DisplayThreadInfo>> display_threads_;
    
    // Helper function to fetch file content from server
    std::string FetchFileContent(const std::string& filename);
    
    // Thread function for continuous display updates
    static void DisplayThreadFunction(CsvClient* client, DisplayThreadInfo* thread_info);
};
