// client/csv_client.cpp: Implements the CsvClient class methods.
#include "csv_client.hpp"
#include "utils/file_utils.hpp" // For reading file content

#include <iostream>
#include <fstream> // Only needed for error check now, maybe remove later
#include <stdexcept> // For exception handling from read_file

// Constructor
CsvClient::CsvClient(std::shared_ptr<Channel> channel)
    : stub_(CsvService::NewStub(channel)) {}

// UploadCsv implementation
bool CsvClient::UploadCsv(const std::string& filename) {
    std::string file_contents;
    try {
        file_contents = file_utils::read_file(filename);
    } catch (const std::runtime_error& e) {
        std::cerr << "Error reading file: " << e.what() << std::endl;
        return false;
    }

    CsvUploadRequest request;
    request.set_filename(filename); // Use base filename or full path?
    request.set_csv_data(file_contents);

    CsvUploadResponse response;
    ClientContext context;
    Status status = stub_->UploadCsv(&context, request, &response);

    if (status.ok()) {
        std::cout << "Upload successful: " << response.message() << std::endl;
        std::cout << "Rows: " << response.row_count() 
                  << ", Columns: " << response.column_count() << std::endl;
        return true;
    } else {
        std::cerr << "Upload failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
        return false;
    }
}

// ListFiles implementation
void CsvClient::ListFiles() {
    Empty request;
    CsvFileList response;
    ClientContext context;

    Status status = stub_->ListLoadedFiles(&context, request, &response);

    if (status.ok()) {
        std::cout << "Loaded files on server:" << std::endl;
        if (response.filenames_size() == 0) {
            std::cout << "  (None)" << std::endl;
        } else {
            for (const std::string& name : response.filenames()) {
                std::cout << "  - " << name << std::endl;
            }
        }
    } else {
        std::cerr << "ListFiles failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
}
