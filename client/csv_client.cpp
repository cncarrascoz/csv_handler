// client/csv_client.cpp: Implements the CsvClient class methods.
#include "csv_client.hpp"
#include "utils/file_utils.hpp" // For reading file content

#include <iostream>
#include <fstream> // Only needed for error check now, maybe remove later
#include <stdexcept> // For exception handling from read_file
#include <sstream>   // For parsing comma-separated values
#include <thread>
#include <mutex>
#include <chrono>
#include <atomic>
#include <unordered_map>

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

    // Extract just the base filename from the path
    std::string base_filename = filename;
    size_t last_slash_pos = filename.find_last_of("/\\");
    if (last_slash_pos != std::string::npos) {
        base_filename = filename.substr(last_slash_pos + 1);
    }

    CsvUploadRequest request;
    request.set_filename(base_filename); // Use only the base filename
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

// Destructor to clean up display threads
CsvClient::~CsvClient() {
    StopAllDisplayThreads();
}

// Helper function to fetch file content from server
std::string CsvClient::FetchFileContent(const std::string& filename) {
    ViewFileRequest request;
    request.set_filename(filename);
    
    ViewFileResponse response;
    ClientContext context;
    
    Status status = stub_->ViewFile(&context, request, &response);
    
    if (status.ok() && response.success()) {
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
        
        // Format the table
        return file_utils::format_csv_as_table(column_names, rows);
    }
    
    return "Error fetching file content: " + 
           (status.ok() ? response.message() : status.error_message());
}

// Opens a new terminal window that displays the CSV file and updates in real-time
void CsvClient::DisplayFile(const std::string& filename) {
    // Check if the file exists on the server first
    ViewFileRequest request;
    request.set_filename(filename);
    
    ViewFileResponse response;
    ClientContext context;
    
    Status status = stub_->ViewFile(&context, request, &response);
    
    if (!status.ok() || !response.success()) {
        std::cerr << "Failed to display file: " << 
                  (status.ok() ? response.message() : status.error_message()) << std::endl;
        return;
    }
    
    // Check if we already have a display thread for this file
    {
        std::lock_guard<std::mutex> lock(display_threads_mutex_);
        auto it = display_threads_.find(filename);
        if (it != display_threads_.end()) {
            std::cout << "Display already active for file '" << filename << "'" << std::endl;
            return;
        }
    }
    
    // Create a new display thread info
    auto thread_info = std::make_unique<DisplayThreadInfo>();
    thread_info->running.store(true);
    thread_info->filename = filename;
    
    // Fetch initial content
    thread_info->last_content = FetchFileContent(filename);
    
    // Create a temporary file for the display
    std::string temp_filename = "/tmp/csv_display_" + filename + ".txt";
    std::ofstream temp_file(temp_filename);
    if (!temp_file) {
        std::cerr << "Failed to create temporary file for display" << std::endl;
        return;
    }
    
    // Write initial content
    temp_file << "CSV Display: " << filename << "\n";
    temp_file << "Real-time updates enabled. Press Ctrl+C to close this window.\n\n";
    temp_file << thread_info->last_content;
    temp_file.close();
    
    // Open a new terminal window to display the file
    std::string display_command;
    
    #ifdef _WIN32
    display_command = "start cmd /c \"type " + temp_filename + " && pause\"";
    #else
    // On macOS/Linux, use a loop to continuously update the display
    display_command = "osascript -e 'tell app \"Terminal\" to do script \"clear && cat " + 
                      temp_filename + " && echo \\\"\\nUpdating live...\\\" && " +
                      "while true; do sleep 2; clear; cat " + temp_filename + 
                      "; echo \\\"\\nUpdating live...\\\"; done\"'";
    #endif
    
    system(display_command.c_str());
    
    // Start the update thread
    DisplayThreadInfo* thread_info_ptr = thread_info.get();
    thread_info->thread = std::thread(DisplayThreadFunction, this, thread_info_ptr);
    
    // Store the thread info
    {
        std::lock_guard<std::mutex> lock(display_threads_mutex_);
        display_threads_[filename] = std::move(thread_info);
    }
    
    std::cout << "Display started for file '" << filename << "'" << std::endl;
    std::cout << "Updates will appear in the new terminal window" << std::endl;
}

// Thread function for continuous display updates
void CsvClient::DisplayThreadFunction(CsvClient* client, DisplayThreadInfo* thread_info) {
    const std::string& filename = thread_info->filename;
    std::string temp_filename = "/tmp/csv_display_" + filename + ".txt";
    
    while (thread_info->running.load()) {
        // Sleep for a short time between updates
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // Fetch the latest content
        std::string new_content = client->FetchFileContent(filename);
        
        // Check if content has changed
        bool content_changed = false;
        {
            std::lock_guard<std::mutex> lock(thread_info->content_mutex);
            if (new_content != thread_info->last_content) {
                thread_info->last_content = new_content;
                content_changed = true;
            }
        }
        
        if (content_changed) {
            // Update the temporary file
            std::ofstream temp_file(temp_filename);
            if (temp_file) {
                temp_file << "CSV Display: " << filename << "\n";
                temp_file << "Real-time updates enabled. Press Ctrl+C to close this window.\n\n";
                temp_file << new_content;
                temp_file.close();
            }
        }
    }
}

// Stops the display thread for a specific file
void CsvClient::StopDisplayThread(const std::string& filename) {
    std::lock_guard<std::mutex> lock(display_threads_mutex_);
    auto it = display_threads_.find(filename);
    if (it != display_threads_.end()) {
        it->second->running.store(false);
        if (it->second->thread.joinable()) {
            it->second->thread.join();
        }
        display_threads_.erase(it);
        std::cout << "Display stopped for file '" << filename << "'" << std::endl;
    } else {
        std::cout << "No active display for file '" << filename << "'" << std::endl;
    }
}

// Stops all display threads
void CsvClient::StopAllDisplayThreads() {
    std::lock_guard<std::mutex> lock(display_threads_mutex_);
    for (auto& pair : display_threads_) {
        pair.second->running.store(false);
        if (pair.second->thread.joinable()) {
            pair.second->thread.join();
        }
    }
    display_threads_.clear();
}
