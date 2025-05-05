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
#include <grpcpp/create_channel.h> // Needed for grpc::CreateChannel
#include <chrono> // Added chrono include

using csvservice::ClusterStatusResponse;
using csvservice::ViewFileRequest;
using csvservice::ViewFileResponse;

// Implementation for the RPC helper template function
template<typename StubMethod, typename RequestType, typename ResponseType>
Status CsvClient::MakeRpcCallWithRetry(
    StubMethod method_ptr,
    const RequestType& request,
    ResponseType* response
) {
    // std::cout << "[RPC Debug] Entering MakeRpcCallWithRetry for method." << std::endl;
    // 1. Ensure connection
    if (!stub_) {
        // std::cout << "[RPC Debug] No active stub. Attempting connect()..." << std::endl;
        if (!connect()) {
            // Return specific status if connection failed entirely
            return Status(grpc::StatusCode::UNAVAILABLE, "Cannot make RPC call, no connection available.");
        }
        // If connect() succeeded, stub_ should now be valid
        if (!stub_) {
             // Should not happen if connect() returns true, but defensive check
             return Status(grpc::StatusCode::INTERNAL, "Connection established but stub is invalid.");
        }
    }

    // 2. Initial RPC Call
    ClientContext context;
    // Consider making the timeout configurable or longer
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(5);
    context.set_deadline(deadline);

    // Call the gRPC method using the provided pointer-to-member function
    // std::cout << "[RPC Debug] Attempting RPC on current server: " << current_server_address_ << std::endl;
    Status status = (stub_.get()->*method_ptr)(&context, request, response);

    // 3. Handle failure and potential retry
    if (!status.ok()) {
        std::cerr << "Initial RPC failed on " << current_server_address_ << ": "
                  << status.error_code() << ": " << status.error_message() << std::endl;

        // Check if the error warrants a retry (connection/timeout issues)
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
            status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
            status.error_code() == grpc::StatusCode::INTERNAL) { // Also retry on some internal errors?

            // std::cout << "[RPC Debug] Retrying RPC call after failure. Attempting connect() for new server..." << std::endl;
            // connect() tries the next available server
            if (connect()) {
                 // Ensure stub is valid after reconnect attempt
                if (!stub_) {
                    return Status(grpc::StatusCode::INTERNAL, "Reconnected but stub is invalid.");
                }

                // Retry the RPC call ONCE with a new context
                ClientContext retry_context;
                std::chrono::system_clock::time_point retry_deadline =
                    std::chrono::system_clock::now() + std::chrono::seconds(5); // Use same timeout for retry
                retry_context.set_deadline(retry_deadline);

                // std::cout << "[RPC Debug] Retrying RPC on newly connected server: " << current_server_address_ << "..." << std::endl;
                // Retry the call
                status = (stub_.get()->*method_ptr)(&retry_context, request, response);

                if (!status.ok()) {
                    std::cerr << "Retry failed on " << current_server_address_ << ": "
                              << status.error_code() << ": " << status.error_message() << std::endl;
                } else {
                    //  std::cout << "[RPC Debug] Retry successful on " << current_server_address_ << "." << std::endl;
                }
            } else {
                // std::cerr << "[RPC Debug] Failed to reconnect to any server during retry. Aborting." << std::endl;
                // Keep the original error status if reconnection failed
            }
        } else {
             std::cout << "Non-retryable error encountered." << std::endl;
        }
    }

    // 4. Return the final status (could be OK from initial call, OK from retry, or error)
    return status;
}

// NOTE: Template implementations are often put in headers or .tpp files,
// but since this is a private helper only called within this .cpp file,
// defining it here should be acceptable.

CsvClient::CsvClient(const std::vector<std::string>& server_addresses) {
    // Initialize server list and attempt initial connection
    server_addresses_ = server_addresses;
    current_server_index_ = 0;
    connect(); // Attempt initial connection
}

CsvClient::~CsvClient() {
    // Cleanup logic if needed (e.g., stopping threads)
}

// Implementation for the connection logic
bool CsvClient::connect() {
    if (server_addresses_.empty()) {
        std::cerr << "Error: No server addresses configured." << std::endl;
        return false;
    }
    // std::cout << "[Connect Debug] Entering connect(). Current index: " << current_server_index_ << std::endl;

    std::lock_guard<std::mutex> lock(reconnect_mutex_); // Ensure only one thread reconnects

    // Try connecting to each server address in the list, starting from the current index
    size_t attempts = 0;
    while (attempts < server_addresses_.size()) {
        current_server_address_ = server_addresses_[current_server_index_];
        // std::cout << "[Connect Debug] Attempting connection to: " << current_server_address_
                //   << " (Attempt " << attempts + 1 << "/" << server_addresses_.size() << ")" << std::endl;

        // Create a new channel for the current attempt
        channel_ = grpc::CreateChannel(current_server_address_, grpc::InsecureChannelCredentials());

        // Check if the channel is connected within a short timeout
        grpc_connectivity_state state = channel_->GetState(true); // Request immediate check
        // Wait for a short period for the connection to establish or fail definitively
        // Adjust timeout as needed
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(2); 
        if (channel_->WaitForConnected(deadline)) {
            stub_ = CsvService::NewStub(channel_);
            std::cout << "Connected successfully to " << current_server_address_ << "." << std::endl;
            // std::cout << "[Connect Debug] Successfully connected to: " << current_server_address_ << std::endl;
            return true; // Connection successful
        } else {
             std::cerr << "Failed to connect to " << current_server_address_ << " (timeout or error)." << std::endl;
            //  std::cerr << "[Connect Debug] Connection failed for: " << current_server_address_ << ". Moving to next server." << std::endl;
            // Move to the next server index, wrapping around
            current_server_index_ = (current_server_index_ + 1) % server_addresses_.size();
            attempts++;
             // Reset channel and stub for the next attempt
            stub_.reset(); 
            channel_.reset(); 
        }
    }

    std::cerr << "Error: Failed to connect to any server after trying all addresses." << std::endl;
    // std::cerr << "[Connect Debug] Exhausted all server addresses. Connection failed." << std::endl;
    current_server_address_ = ""; // Indicate no connection
    stub_.reset();
    channel_.reset();
    return false; // Failed to connect to any server
}

// Create a channel to a specific server address
std::shared_ptr<Channel> CsvClient::create_channel(const std::string& server_address) {
    return grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
}

// Try to reconnect to another server if current connection fails
bool CsvClient::try_reconnect() {
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
        
        auto channel = create_channel(next_address);
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
    CsvFileList reply;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::ListLoadedFiles, request, &reply);
    if (!status.ok()) {
        std::cerr << "TestConnection failed: " << status.error_code() << ": " 
                  << status.error_message() << std::endl;
    }
    return status.ok(); // Return true if the call (or retry) succeeded
}

// Get cluster status from the server
bool CsvClient::get_cluster_status(std::string& leader, std::vector<std::string>& servers, int& active_count) {
    Empty request;
    ClusterStatusResponse response;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::GetClusterStatus, request, &response);
    
    if (status.ok()) {
        leader = response.leader_address();
        servers.clear();
        for (const auto& server : response.server_addresses()) {
            servers.push_back(server);
        }
        active_count = response.active_server_count(); // Correct field name
        return true;
    } else {
        std::cerr << "GetClusterStatus failed: " << status.error_code() << ": " 
                  << status.error_message() << " on " << current_server_address_ << std::endl;
        return false;
    }
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
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::UploadCsv, request, &response);

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
    CsvFileList reply;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::ListLoadedFiles, request, &reply);

    if (status.ok()) {
        std::cout << "Loaded files on server (" << current_server_address_ << "):" << std::endl;
        if (reply.filenames_size() == 0) {
            std::cout << "  (No files loaded)" << std::endl;
        } else {
            for (const std::string& name : reply.filenames()) {
                std::cout << "  - " << name << std::endl;
            }
        }
    } else {
        std::cerr << "Error: Could not list files. Last error from " << current_server_address_ 
                  << ", code: " << status.error_code() << std::endl;
    }
}

// View file contents
void CsvClient::ViewFile(const std::string& filename) {
    ViewFileRequest request;
    request.set_filename(filename);
    ViewFileResponse response;
    // Call the RPC using the helper function
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::ViewFile, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
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

            std::cout << "Content of " << filename << " (from server " << current_server_address_ << ") :" << std::endl;
            std::string formatted_table = ::file_utils::format_csv_as_table(column_names, rows);
            std::cout << formatted_table << std::endl;
        } else {
             std::cerr << "Server error viewing " << filename << " on " << current_server_address_ << ": " << response.message() << std::endl;
        }
    } else {
         std::cerr << "ViewFile failed for " << filename 
                   << ". Last status from " << current_server_address_ 
                   << ": " << status.error_code() << ": " << status.error_message() << std::endl;
    }
}

// ComputeSum implementation
void CsvClient::ComputeSum(const std::string& filename, const std::string& column_name) {
    ColumnOperationRequest request;
    request.set_filename(filename);
    request.set_column_name(column_name);
    
    NumericResponse response;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::ComputeSum, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Sum of column '" << column_name << "' in file '" << filename << "' (from server " << current_server_address_ << "): " 
                      << response.value() << std::endl;
        } else {
            std::cerr << "Failed to compute sum: " << response.message() << std::endl;
        }
    } else {
         std::cerr << "ComputeSum failed for " << filename << ", column " << column_name
                   << ". Last status from " << current_server_address_ 
                   << ": " << status.error_code() << ": " << status.error_message() << std::endl;
    }
}

// ComputeAverage implementation
void CsvClient::ComputeAverage(const std::string& filename, const std::string& column_name) {
    ColumnOperationRequest request;
    request.set_filename(filename);
    request.set_column_name(column_name);
    
    NumericResponse response;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::ComputeAverage, request, &response);
    
    if (status.ok()) {
        if (response.success()) {
            std::cout << "Average of column '" << column_name << "' in file '" << filename << "' (from server " << current_server_address_ << "): " 
                      << response.value() << std::endl;
        } else {
            std::cerr << "Failed to compute average: " << response.message() << std::endl;
        }
    } else {
         std::cerr << "ComputeAverage failed for " << filename << ", column " << column_name
                   << ". Last status from " << current_server_address_ 
                   << ": " << status.error_code() << ": " << status.error_message() << std::endl;
    }
}

// InsertRow implementation
bool CsvClient::InsertRow(const std::string& filename, const std::vector<std::string>& row_data) {
    InsertRowRequest request;
    request.set_filename(filename);
    for (const auto& value : row_data) {
        request.add_values(value); // Correct function name
    }
    
    ModificationResponse response;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::InsertRow, request, &response);
    
    if (status.ok()) {
        std::cout << "InsertRow successful for " << filename << " on server " << current_server_address_ << ": " << response.message() << std::endl;
        return true;
    } else {
        std::cerr << "InsertRow failed for " << filename 
                  << ". Last status from " << current_server_address_ 
                  << ": " << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }
}

// DeleteRow implementation
void CsvClient::DeleteRow(const std::string& filename, int row_index) {
    DeleteRowRequest request;
    request.set_filename(filename);
    request.set_row_index(row_index);
    
    ModificationResponse response;
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::DeleteRow, request, &response);
    
    if (status.ok()) {
        std::cout << "DeleteRow successful for " << filename << ", row " << row_index << " on server " << current_server_address_ << ": " << response.message() << std::endl;
    } else {
        std::cerr << "DeleteRow failed for " << filename << ", row " << row_index
                  << ". Last status from " << current_server_address_ 
                  << ": " << status.error_code() << ": " << status.error_message() << std::endl;
    }
}

// Helper function to fetch file content from server using retry logic
std::string CsvClient::FetchFileContent(const std::string& filename) {
    ViewFileRequest request;
    request.set_filename(filename);
    ViewFileResponse response;

    // Use the retry helper
    Status status = MakeRpcCallWithRetry(&CsvService::Stub::ViewFile, request, &response);

    if (status.ok() && response.success()) {
        // Format the data into a single string representation
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
        return ::file_utils::format_csv_as_table(column_names, rows);
    } else if (!status.ok()) {
        return "Error: RPC failed fetching content for " + filename + " from " + current_server_address_ + ": " + status.error_message();
    } else { // status.ok() but !response.success()
        return "Error: Server error fetching content for " + filename + " from " + current_server_address_ + ": " + response.message();
    }
}

// Thread function for continuous display updates
void CsvClient::DisplayThreadFunction(DisplayThreadInfo* thread_info) {
    std::string temp_filename = "/tmp/csv_display_" + thread_info->filename + ".txt";
 
    // Periodically update the display 
    while (thread_info->running.load()) { 
        // Fetch cluster status 
        std::string leader; 
        std::vector<std::string> servers; 
        int active_count = 0; 
        get_cluster_status(leader, servers, active_count); 
 
        // Fetch file content from server 
        std::string current_content = FetchFileContent(thread_info->filename); 
 
        // Combine status and content 
        std::stringstream display_output; 
        display_output << "--- Cluster Status ---\n"; 
        display_output << "Leader: " << (leader.empty() ? "(None)" : leader) << "\n"; 
        display_output << "Active Servers: " << active_count << " / " << server_addresses_.size() << "\n"; 
        display_output << "\n--- File Content: " << thread_info->filename << " ---\n"; 
        display_output << current_content << "\n"; 
 
        std::string display_str = display_output.str();
         
        // Update the temporary file only if content or status has changed 
        { // Scope for mutex lock 
            std::lock_guard<std::mutex> lock(thread_info->content_mutex); 
            if (display_str != thread_info->last_content) { 
                thread_info->last_content = display_str; 
                 
                // Reopen the file in truncate mode to clear it before writing 
                std::ofstream temp_file(temp_filename, std::ios::trunc); 
                if (temp_file) { 
                    // Write clear screen sequence + new content 
                    temp_file << "\033[2J\033[H"; // ANSI clear screen and move cursor to top-left 
                    temp_file << display_str; 
                    temp_file.flush(); // Ensure content is written for tail -f 
                } 
            } 
        }
         
        // Wait for a short period before fetching again
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

// Stops the display thread for a specific file
void CsvClient::StopDisplayThread(const std::string& filename) {
    std::lock_guard<std::mutex> lock(display_threads_mutex_);
    
    auto it = display_threads_.find(filename);
    if (it != display_threads_.end()) {
        // Signal the thread to stop
        it->second->running.store(false);
        
        // Wait for the thread to finish
        if (it->second->thread.joinable()) {
            it->second->thread.join();
        }
        
        // Remove the temporary file
        std::string temp_filename = "/tmp/csv_display_" + filename + ".txt";
        std::remove(temp_filename.c_str());
        
        // Remove the thread info
        display_threads_.erase(it);
        
        std::cout << "Stopped displaying file: " << filename << std::endl;
    } else {
        std::cerr << "File not being displayed: " << filename << std::endl;
    }
}

// Stops all display threads
void CsvClient::StopAllDisplayThreads() {
    std::lock_guard<std::mutex> lock(display_threads_mutex_);
    
    for (auto& pair : display_threads_) {
        // Signal the thread to stop
        pair.second->running.store(false);
        
        // Wait for the thread to finish
        if (pair.second->thread.joinable()) {
            pair.second->thread.join();
        }
        
        // Remove the temporary file
        std::string temp_filename = "/tmp/csv_display_" + pair.first + ".txt";
        std::remove(temp_filename.c_str());
    }
    
    // Clear the map
    display_threads_.clear();
}

// Opens a new terminal window that displays the CSV file and updates in real-time
void CsvClient::DisplayFile(const std::string& filename) {
    std::lock_guard<std::mutex> lock(display_threads_mutex_);
    
    // Check if we already have a display thread for this file
    if (display_threads_.find(filename) != display_threads_.end()) {
        std::cout << "Already displaying file: " << filename << std::endl;
        return;
    }
    
    // Create a new display thread info
    auto thread_info = std::make_unique<DisplayThreadInfo>();
    thread_info->running.store(true);
    thread_info->filename = filename;
    
    // Create a temporary file for the display
    std::string temp_filename = "/tmp/csv_display_" + filename + ".txt";
    std::ofstream temp_file(temp_filename);
    if (!temp_file) {
        std::cerr << "Failed to create temporary file for display" << std::endl;
        return;
    }
    
    // Write initial content to the temporary file
    temp_file << "CSV Display: " << filename << "\n";
    temp_file << "Loading content...\n";
    temp_file.close();
    
    // Start the display thread
    thread_info->thread = std::thread(&CsvClient::DisplayThreadFunction, this, thread_info.get());
    
    // Open the temporary file in a new terminal window
    std::string open_command;
#ifdef __APPLE__
    // Use osascript to open a new Terminal window and run tail -f
    open_command = "osascript -e 'tell application \"Terminal\" to do script \"tail -f " + temp_filename + "\"'";
#else
    // Assume Linux/X11 with xterm
    open_command = "xterm -e \"tail -f " + temp_filename + "\" &";
#endif
    
    int result = system(open_command.c_str());
    if (result != 0) {
        std::cerr << "Failed to open display terminal" << std::endl;
        thread_info->running.store(false);
        thread_info->thread.join();
        return;
    }
    
    // Store the thread info
    display_threads_[filename] = std::move(thread_info);
    
    std::cout << "Displaying file: " << filename << " in a new terminal window" << std::endl;
}
