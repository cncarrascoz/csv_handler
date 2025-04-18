// client/csv_client.hpp: Defines the CsvClient class for interacting with the server.
#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
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
    CsvClient(std::shared_ptr<Channel> channel);
    ~CsvClient(); // Destructor to clean up display threads

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
    
    // Sends a new row to append to a file on the server.
    void InsertRow(const std::string& filename, const std::string& comma_separated_values);
    
    // Requests deletion of a specific row from a file on the server.
    void DeleteRow(const std::string& filename, int row_index);
    
    // Opens a new terminal window that displays the CSV file and updates in real-time
    void DisplayFile(const std::string& filename);
    
    // Stops the display thread for a specific file
    void StopDisplayThread(const std::string& filename);
    
    // Stops all display threads
    void StopAllDisplayThreads();

private:
    std::unique_ptr<CsvService::Stub> stub_;
    
    // Thread management for display command
    struct DisplayThreadInfo {
        std::thread thread;
        std::atomic<bool> running;
        std::string filename;
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
