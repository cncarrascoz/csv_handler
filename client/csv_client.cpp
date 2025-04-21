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

// Constructor for single server
CsvClient::CsvClient(std::shared_ptr<Channel> channel)
    : stub_(CsvService::NewStub(channel)) {
    // Store a default server address for backward compatibility
    server_addresses_.push_back("unknown");
    current_server_address_ = "unknown";
}

// Constructor for multiple servers with fault tolerance
CsvClient::CsvClient(const std::vector<std::string>& server_addresses) 
    : server_addresses_(server_addresses) {
    if (server_addresses.empty()) {
        throw std::runtime_error("No server addresses provided");
    }
    
    // Connect to the first server initially
    current_server_index_ = 0;
    current_server_address_ = server_addresses[0];
    auto channel = CreateChannel(current_server_address_);
    stub_ = CsvService::NewStub(channel);
    
    std::cout << "Connected to server: " << current_server_address_ << std::endl;
}

// Create a channel to a specific server address
std::shared_ptr<Channel> CsvClient::CreateChannel(const std::string& server_address) {
    return grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
}

// Try to reconnect to another server if current connection fails
bool CsvClient::TryReconnect() {
    std::lock_guard<std::mutex> lock(reconnect_mutex_);
    
    // If we only have one server or no servers, can't reconnect
    if (server_addresses_.size() <= 1) {
        std::cerr << "No alternative servers available for reconnection" << std::endl;
        return false;
    }
    
    std::cout << "Current server " << current_server_address_ << " is unavailable. Attempting to reconnect to another server..." << std::endl;
    
    // Try each server in the list
    for (size_t i = 0; i < server_addresses_.size(); i++) {
        // Skip the current server that failed
        if (i == current_server_index_) continue;
        
        std::string next_address = server_addresses_[i];
        std::cout << "Attempting to connect to server: " << next_address << std::endl;
        
        auto channel = CreateChannel(next_address);
        auto new_stub = CsvService::NewStub(channel);
        
        // Test if we can connect to this server
        Empty request;
        CsvFileList response;
        ClientContext context;
        
        // Set a short deadline for the connection attempt
        std::chrono::system_clock::time_point deadline = 
            std::chrono::system_clock::now() + std::chrono::seconds(2);
        context.set_deadline(deadline);
        
        Status status = new_stub->ListLoadedFiles(&context, request, &response);
        
        if (status.ok()) {
            // Connection successful, update the client state
            stub_ = std::move(new_stub);
            current_server_index_ = i;
            current_server_address_ = next_address;
            std::cout << "Successfully reconnected to server: " << next_address << std::endl;
            return true;
        }
    }
    
    std::cerr << "Failed to connect to any available server. Please ensure at least one server is running." << std::endl;
    return false;
}

// Tests if the client can connect to the server
bool CsvClient::TestConnection() {
    Empty request;
    CsvFileList response;
    ClientContext context;
    
    // Set a deadline for the connection attempt (3 seconds)
    std::chrono::system_clock::time_point deadline = 
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    
    Status status = stub_->ListLoadedFiles(&context, request, &response);
    
    if (!status.ok() && server_addresses_.size() > 1) {
        // Connection failed, try to reconnect to another server
        return TryReconnect();
    }
    
    return status.ok();
}

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
        if (server_addresses_.size() > 1) {
            // Upload failed, try to reconnect to another server
            if (TryReconnect()) {
                return UploadCsv(filename); // Try the upload again
            }
        }
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
        if (server_addresses_.size() > 1) {
            // ListFiles failed, try to reconnect to another server
            if (TryReconnect()) {
                ListFiles(); // Try the list again
            }
        }
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
        if (server_addresses_.size() > 1) {
            // ViewFile failed, try to reconnect to another server
            if (TryReconnect()) {
                ViewFile(filename); // Try the view again
            }
        }
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
        if (server_addresses_.size() > 1) {
            // ComputeSum failed, try to reconnect to another server
            if (TryReconnect()) {
                ComputeSum(filename, column_name); // Try the computation again
            }
        }
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
        if (server_addresses_.size() > 1) {
            // ComputeAverage failed, try to reconnect to another server
            if (TryReconnect()) {
                ComputeAverage(filename, column_name); // Try the computation again
            }
        }
    }
}

// InsertRow implementation
bool CsvClient::InsertRow(const std::string& filename, const std::string& row_data) {
    InsertRowRequest request;
    request.set_filename(filename);
    
    // Parse the comma-separated values
    std::istringstream iss(row_data);
    std::string value;
    while (std::getline(iss, value, ',')) {
        request.add_values(value);
    }
    
    ModificationResponse response;
    ClientContext context;
    
    Status status = stub_->InsertRow(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Row inserted successfully." << std::endl;
            return true;
        } else {
            std::cerr << "Server rejected insert: " << response.message() << std::endl;
            return false;
        }
    } else {
        std::cerr << "InsertRow failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
        if (server_addresses_.size() > 1) {
            // InsertRow failed, try to reconnect to another server
            if (TryReconnect()) {
                return InsertRow(filename, row_data); // Try the insertion again
            }
        }
        return false;
    }
}

// DeleteRow implementation
bool CsvClient::DeleteRow(const std::string& filename, int row_index) {
    DeleteRowRequest request;
    request.set_filename(filename);
    request.set_row_index(row_index);
    
    ModificationResponse response;
    ClientContext context;
    
    Status status = stub_->DeleteRow(&context, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Row deleted successfully." << std::endl;
            return true;
        } else {
            std::cerr << "Server rejected delete: " << response.message() << std::endl;
            return false;
        }
    } else {
        std::cerr << "DeleteRow failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
        if (server_addresses_.size() > 1) {
            // DeleteRow failed, try to reconnect to another server
            if (TryReconnect()) {
                return DeleteRow(filename, row_index); // Try the deletion again
            }
        }
        return false;
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
    
    // Set a timeout for the RPC call
    std::chrono::system_clock::time_point deadline = 
        std::chrono::system_clock::now() + std::chrono::seconds(2); // 2 second timeout
    context.set_deadline(deadline);

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
            
            // Use the table formatting function
            return file_utils::format_csv_as_table(column_names, rows);
            
        } else {
            return "Error: " + response.message();
        }
    } else {
        // Handle specific error codes if needed
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
             return "Error fetching file content: Timeout contacting server at " + current_server_address_;
        }
        // If connection failed, try to reconnect to another server
        if (server_addresses_.size() > 1 && (status.error_code() == grpc::StatusCode::UNAVAILABLE || status.error_code() == grpc::StatusCode::INTERNAL)) {
            std::cout << "\n[Display] Connection issue with " << current_server_address_ << ". Attempting reconnect..." << std::endl;
            if (TryReconnect()) {
                std::cout << "[Display] Reconnected to " << current_server_address_ << ". Retrying fetch..." << std::endl;
                // Retry with the new connection
                return FetchFileContent(filename);
            }
             return "Error fetching file content: Failed to connect to any server.";
        }
        
        return "Error fetching file content: " + status.error_message() + " (Code: " + std::to_string(status.error_code()) + ")";
    }
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
    thread_info->temp_filename = temp_filename; // Store the temp filename
    
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
    const std::string& temp_filename = thread_info->temp_filename;
    std::string previous_content;
    int previous_active_servers = -1; // Initialize to -1 to force initial write
    size_t total_servers = client->server_addresses_.size();
    
    while (thread_info->running) {
        try {
            // 1. Check active servers
            int active_servers = 0;
            if (total_servers > 0) { // Only check if there are servers defined
                for (size_t i = 0; i < total_servers; ++i) {
                    try {
                        auto channel = client->CreateChannel(client->server_addresses_[i]);
                        auto temp_stub = CsvService::NewStub(channel);
                        
                        Empty request;
                        CsvFileList response;
                        ClientContext context;
                        std::chrono::system_clock::time_point deadline = 
                            std::chrono::system_clock::now() + std::chrono::milliseconds(500); // Short timeout
                        context.set_deadline(deadline);
                        
                        Status status = temp_stub->ListLoadedFiles(&context, request, &response);
                        if (status.ok()) {
                            active_servers++;
                        }
                    } catch (const std::exception& e) {
                        // Ignore connection errors for individual server checks
                    }
                }
            }

            // 2. Fetch current content
            std::string current_content = client->FetchFileContent(filename);
            bool content_is_error = current_content.rfind("Error", 0) == 0; // Check if content is an error message

            // 3. Update temp file if content or server count changed
            if (current_content != previous_content || active_servers != previous_active_servers) {
                std::ofstream temp_file(temp_filename, std::ios::trunc); // Overwrite the file
                if (temp_file) {
                    // Write header
                    temp_file << "CSV Display: " << filename << "\n";
                    temp_file << "Active Servers: " << active_servers << "/" << total_servers << "\n";
                    temp_file << "Real-time updates enabled. Press Ctrl+C in this window to stop.\n\n";
                    
                    // Write content or error message
                    temp_file << current_content;
                    temp_file.close();
                    
                    previous_content = current_content;
                    previous_active_servers = active_servers;
                } else {
                    // Log error locally, can't update the temp file
                    std::cerr << "Error: Could not open temp file " << temp_filename << " for writing." << std::endl;
                }

                // Handle fetch error - potentially trigger reconnect in main client context
                if (content_is_error && current_content.find("Connection refused") != std::string::npos) {
                     // We don't call TryReconnect directly here as it might interfere with the main client's state.
                     // The FetchFileContent already handles trying to reconnect if the primary connection fails.
                     // We just log it here for the display thread context.
                     std::ofstream temp_file(temp_filename, std::ios::app); // Append error state
                     if(temp_file) {
                        temp_file << "\n\n[Server connection issue detected. Client is attempting to reconnect...]\n";
                        temp_file.close();
                     }
                }
            }

            // Sleep before next update cycle
            std::this_thread::sleep_for(std::chrono::seconds(1)); // Update interval
            
        } catch (const std::exception& e) {
            std::cerr << "Error in display thread for " << filename << ": " << e.what() << std::endl;
            // Log error to the temp file as well
            try {
                 std::ofstream temp_file(temp_filename, std::ios::app);
                 if (temp_file) {
                    temp_file << "\n\n[Error in display thread: " << e.what() << "]\n";
                    temp_file.close();
                 }
            } catch (...) { /* Ignore errors writing error message */ }

            // Sleep longer after an error before retrying
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    }
    
    std::cout << "Display thread for " << filename << " stopping." << std::endl;
    // Clean up the temp file when the thread stops
    std::remove(temp_filename.c_str());
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
