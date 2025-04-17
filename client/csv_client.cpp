// client/csv_client.cpp: Implements the CsvClient class methods.
#include "csv_client.hpp"
#include "utils/file_utils.hpp" // For reading file content

#include <iostream>
#include <fstream> // Only needed for error check now, maybe remove later
#include <stdexcept> // For exception handling from read_file
#include <sstream>   // For parsing comma-separated values

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

// ViewFile implementation
void CsvClient::ViewFile(const std::string& filename) {
    ViewFileRequest request;
    request.set_filename(filename);
    
    ViewFileResponse response;
    ClientContext context;
    
    Status status = stub_->ViewFile(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            // Convert protobuf data to format needed by format_csv_as_table
            std::vector<std::string> column_names;
            for (int i = 0; i < response.column_names_size(); ++i) {
                column_names.push_back(response.column_names(i));
            }
            
            std::vector<std::vector<std::string>> rows;
            for (const auto& row_proto : response.rows()) {
                std::vector<std::string> row;
                for (int i = 0; i < row_proto.values_size(); ++i) {
                    row.push_back(row_proto.values(i));
                }
                rows.push_back(row);
            }
            
            // Use the new table formatting function
            std::string formatted_table = file_utils::format_csv_as_table(column_names, rows);
            std::cout << formatted_table;
        } else {
            std::cerr << "Failed to view file: " << response.message() << std::endl;
        }
    } else {
        std::cerr << "ViewFile failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
}

// ComputeSum implementation
void CsvClient::ComputeSum(const std::string& filename, const std::string& column_name) {
    ColumnOperationRequest request;
    request.set_filename(filename);
    request.set_column_name(column_name);
    
    NumericResponse response;
    ClientContext context;
    
    Status status = stub_->ComputeSum(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Sum of column '" << column_name << "' in file '" 
                      << filename << "': " << response.value() << std::endl;
        } else {
            std::cerr << "Failed to compute sum: " << response.message() << std::endl;
        }
    } else {
        std::cerr << "ComputeSum failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
}

// ComputeAverage implementation
void CsvClient::ComputeAverage(const std::string& filename, const std::string& column_name) {
    ColumnOperationRequest request;
    request.set_filename(filename);
    request.set_column_name(column_name);
    
    NumericResponse response;
    ClientContext context;
    
    Status status = stub_->ComputeAverage(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Average of column '" << column_name << "' in file '" 
                      << filename << "': " << response.value() << std::endl;
        } else {
            std::cerr << "Failed to compute average: " << response.message() << std::endl;
        }
    } else {
        std::cerr << "ComputeAverage failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
}

// InsertRow implementation
void CsvClient::InsertRow(const std::string& filename, const std::string& comma_separated_values) {
    InsertRowRequest request;
    request.set_filename(filename);
    
    // Parse the comma-separated values
    std::istringstream iss(comma_separated_values);
    std::string value;
    while (std::getline(iss, value, ',')) {
        request.add_values(value);
    }
    
    ModificationResponse response;
    ClientContext context;
    
    Status status = stub_->InsertRow(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Row inserted successfully: " << response.message() << std::endl;
        } else {
            std::cerr << "Failed to insert row: " << response.message() << std::endl;
        }
    } else {
        std::cerr << "InsertRow failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
}

// DeleteRow implementation
void CsvClient::DeleteRow(const std::string& filename, int row_index) {
    DeleteRowRequest request;
    request.set_filename(filename);
    request.set_row_index(row_index);
    
    ModificationResponse response;
    ClientContext context;
    
    Status status = stub_->DeleteRow(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Row deleted successfully: " << response.message() << std::endl;
        } else {
            std::cerr << "Failed to delete row: " << response.message() << std::endl;
        }
    } else {
        std::cerr << "DeleteRow failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
}
