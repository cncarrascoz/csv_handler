#include "csv_service_impl.hpp"
#include "storage/csv_parser.hpp"
#include "core/TableView.hpp"
#include "core/Mutation.hpp"
#include "server_registry.hpp"
#include <grpcpp/grpcpp.h>
#include "storage/InMemoryStateMachine.hpp"
#include <memory>
#include <chrono>
#include <thread>
#include <future>
#include <functional>
#include <iostream>
#include <numeric>
#include <limits>

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using grpc::Channel;
using grpc::ClientContext;

namespace network {

CsvServiceImpl::CsvServiceImpl(ServerRegistry& registry)
    : registry_(registry), state_(std::make_unique<InMemoryStateMachine>()) // Simplified state machine for now
{ 
    // Constructor body - can be empty if initialization is done in member initializers
    // or if registry handles its own lifecycle setup elsewhere (like in main.cpp)
} 

Status CsvServiceImpl::UploadCsv(
    ServerContext* context,
    const csvservice::CsvUploadRequest* request,
    csvservice::CsvUploadResponse* response) {
    
    // --- Leader/Follower Logic --- 
    std::string leader_address = registry_.get_leader();
    std::string self_addr = registry_.get_self_address();

    if (!leader_address.empty() && leader_address != self_addr) { 
        // --- Follower: Forward to Leader --- 
        std::cout << "Forwarding UploadCsv for " << request->filename() << " to leader: " << leader_address << std::endl; 
        auto channel = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
        auto stub = csvservice::CsvService::NewStub(channel);
        
        grpc::ClientContext client_context;
        // Set a reasonable deadline for the forwarded call
        std::chrono::system_clock::time_point deadline = 
            std::chrono::system_clock::now() + std::chrono::seconds(5);
        client_context.set_deadline(deadline);
        
        auto status = stub->UploadCsv(&client_context, *request, response);
        
        if (!status.ok()) {
            std::cerr << "Failed to forward UploadCsv to leader " << leader_address 
                      << ": " << status.error_code() << ": " << status.error_message() << std::endl;
            // Return an error indicating forwarding failed 
            return Status(grpc::StatusCode::UNAVAILABLE, "Failed to forward request to the leader server.");
        }
        std::cout << "Successfully forwarded UploadCsv to leader." << std::endl; 
        return status; // Return the status received from the leader 
    } 
    
    // --- Leader or Standalone: Process Locally and Replicate --- 
    std::cout << "Processing UploadCsv locally for: " << request->filename() << std::endl; 
    
    // 1. Process Locally 
    std::string filename = request->filename();
    std::string csv_data(request->csv_data());
    
    int row_count = 0; 
    int col_count = 0; 
    try { 
        ColumnStore column_store = csv::parse_csv(csv_data); 
        row_count = column_store.columns.empty() ? 0 : column_store.columns.begin()->second.size(); 
        col_count = column_store.column_names.size(); 
        
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
        if (mem_state) {
            mem_state->add_csv_file(filename, column_store.column_names, column_store.columns);
        } else { 
            throw std::runtime_error("Internal error: State machine not available"); 
        } 
         
        // Set successful response for the original client 
        response->set_success(true); 
        response->set_message("CSV file loaded successfully by leader."); 
        response->set_row_count(row_count); 
        response->set_column_count(col_count); 
        std::cout << "Locally processed " << filename << " successfully." << std::endl; 
         
    } catch (const std::exception& e) { 
        std::cerr << "Error processing file " << filename << " locally: " << e.what() << std::endl; 
        response->set_success(false); 
        response->set_message(std::string("Failed to process CSV locally: ") + e.what()); 
        // Don't set row/col count on failure 
        return Status(grpc::StatusCode::INTERNAL, e.what()); 
    } 
     
    // 2. Replicate Asynchronously to Peers (if this is the leader in a cluster)
    if (!leader_address.empty() && leader_address == self_addr) { // Double check we are leader
        std::vector<std::string> peers = registry_.get_all_servers(); // Get all known servers 
        std::cout << "Asynchronously replicating " << filename << " to peers..." << std::endl;

        // Make a copy of the request data needed for replication
        // Create a shared_ptr to manage the request's lifetime across threads
        auto request_copy_ptr = std::make_shared<csvservice::CsvUploadRequest>(*request);

        for (const auto& peer_addr : peers) {
            if (peer_addr == self_addr) continue; // Don't replicate to self

            // Launch replication in a separate thread
            std::thread([peer_addr, request_copy_ptr, self_addr] { // Capture necessary data
                std::cout << "  Replication thread started for: " << peer_addr << std::endl;
                try {
                    auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
                    auto stub = csvservice::CsvService::NewStub(channel);

                    csvservice::ReplicateUploadResponse replicate_response;
                    grpc::ClientContext replicate_context;
                    // Set a timeout for replication call
                    std::chrono::system_clock::time_point replicate_deadline = 
                        std::chrono::system_clock::now() + std::chrono::seconds(5); // Increased timeout slightly
                    replicate_context.set_deadline(replicate_deadline);

                    // Use the captured request_copy_ptr
                    Status replicate_status = stub->ReplicateUpload(&replicate_context, *request_copy_ptr, &replicate_response);

                    if (replicate_status.ok() && replicate_response.success()) {
                        std::cout << "    Successfully replicated " << request_copy_ptr->filename() << " to " << peer_addr << std::endl;
                    } else {
                        std::cerr << "    Failed to replicate " << request_copy_ptr->filename() << " to " << peer_addr 
                                  << ": " << replicate_status.error_code() << ": " 
                                  << replicate_status.error_message() 
                                  << " (Peer response: " << replicate_response.message() << ")" << std::endl;
                        // Consider adding retry logic here or notifying a monitoring system
                    }
                } catch (const std::exception& e) {
                    std::cerr << "    Exception during replication to " << peer_addr << ": " << e.what() << std::endl;
                } catch (...) {
                    std::cerr << "    Unknown exception during replication to " << peer_addr << std::endl;
                }
                std::cout << "  Replication thread finished for: " << peer_addr << std::endl;
            }).detach(); // Detach the thread to run independently
        }
        // Note: Replication completion is not guaranteed when UploadCsv returns
        // Client response is sent before replication finishes.
    }

    // Return OK status to the original client immediately after local processing 
    // (if standalone) or after launching async replication (if leader).
    // Replication happens in the background.
    return Status::OK;
}

Status CsvServiceImpl::ReplicateUpload( 
     ServerContext* context, 
     const csvservice::CsvUploadRequest* request, 
     csvservice::ReplicateUploadResponse* response) {
    
    std::cout << "Received file replication request from leader for: " << request->filename() << std::endl;
    
    // Extract file information from the request
    std::string filename = request->filename(); 
    std::string csv_data(request->csv_data()); 
    
    try { 
        // Parse the CSV data
        ColumnStore column_store = csv::parse_csv(csv_data); 
        
        // Get access to the state machine
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get()); 
        if (mem_state) { 
            // Add the file to the local state machine
            mem_state->add_csv_file(filename, column_store.column_names, column_store.columns); 
            
            // Set success response
            response->set_success(true); 
            response->set_message("File successfully replicated to follower");
            std::cout << "Successfully replicated file: " << filename << " from leader" << std::endl;
            return Status::OK;
        } else { 
            // State machine not available
            response->set_success(false); 
            response->set_message("Internal error: State machine not available"); 
            std::cerr << "Failed to replicate file: State machine unavailable" << std::endl;
            return Status(grpc::StatusCode::INTERNAL, "State machine unavailable");
        } 
    } catch (const std::exception& e) { 
        // Error processing the file
        response->set_success(false); 
        response->set_message(std::string("Replication failed: ") + e.what()); 
        std::cerr << "Error replicating file " << filename << ": " << e.what() << std::endl; 
        return Status(grpc::StatusCode::INTERNAL, e.what()); 
    } 
}

Status CsvServiceImpl::ViewFile(ServerContext* context, 
                                const csvservice::ViewFileRequest* request, 
                                csvservice::ViewFileResponse* response) {
    std::cout << "Processing ViewFile request for: " << request->filename() << std::endl;

    auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
    if (!mem_state) {
        response->set_success(false);
        response->set_message("Internal server error: State machine unavailable.");
        return Status(grpc::StatusCode::INTERNAL, "State machine unavailable");
    }

    const std::string& filename = request->filename();

    if (!mem_state->file_exists(filename)) {
        response->set_success(false);
        response->set_message("File not found on server.");
        std::cerr << "ViewFile error: File not found - " << filename << std::endl;
        return Status(grpc::StatusCode::NOT_FOUND, "File not found");
    }

    try {
        TableView table_view = mem_state->view(filename);

        response->set_success(true);
        response->set_message("File content retrieved successfully.");

        // Populate column names
        for (const auto& name : table_view.column_names) {
            response->add_column_names(name);
        }

        // Populate rows
        size_t num_rows = table_view.row_count();
        for (size_t i = 0; i < num_rows; ++i) {
            csvservice::Row* row_proto = response->add_rows(); // Use Row type
            for (const auto& col_name : table_view.column_names) {
                // Access cell data using column name and row index
                // Use .at() for bounds checking, though direct access [] might be slightly faster if confident
                row_proto->add_values(table_view.columns.at(col_name).at(i));
            }
        }

        std::cout << "Successfully retrieved content for " << filename << std::endl;
        return Status::OK;

    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_message(std::string("Error retrieving file data: ") + e.what());
        std::cerr << "ViewFile exception for " << filename << ": " << e.what() << std::endl;
        return Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

Status CsvServiceImpl::DeleteRow(ServerContext* context, const csvservice::DeleteRowRequest* request, csvservice::ModificationResponse* response) {
    const std::string& filename = request->filename();
    int row_index = request->row_index();

    std::cout << "DeleteRow called for file: " << filename << ", row index: " << row_index << std::endl;

    // Define the handler function to perform the actual deletion
    auto handler = [this, &filename, row_index, response](const csvservice::DeleteRowRequest*, csvservice::ModificationResponse* resp) -> grpc::Status {
        try {
            Mutation mutation;
            mutation.file = filename;
            mutation.op = RowDelete{row_index};
            state_->apply(mutation);

            resp->set_success(true);
            resp->set_message("Row deleted successfully.");
            std::cout << "Successfully deleted row " << row_index << " from " << filename << std::endl;
            
            // Replicate the delete mutation to peers if this node is the leader
            if (registry_.is_leader()) {
                replicate_mutation_async(mutation);
            }

            return Status::OK;
        } catch (const std::exception& e) {
            resp->set_success(false);
            resp->set_message(std::string("Error deleting row: ") + e.what());
            std::cerr << "DeleteRow exception for " << filename << ": " << e.what() << std::endl;
            return Status(grpc::StatusCode::INTERNAL, e.what());
        }
    };

    // Forward if needed, or execute locally if leader
    std::function<grpc::Status(const csvservice::DeleteRowRequest*, csvservice::ModificationResponse*)> handler_func = handler;
    bool forwarded = forward_to_leader_if_needed(request, response, handler_func);
    if (forwarded) {
        // The response is populated by the forward_to_leader_if_needed function upon receiving leader's response
        // Determine status based on response success flag
        std::cout << "DeleteRow request forwarded to leader for file: " << filename << std::endl;
        return response->success() ? Status::OK : Status(grpc::StatusCode::INTERNAL, response->message());
    } else {
        // Executed locally (either leader or standalone)
        return handler(request, response);
    }
}

Status CsvServiceImpl::InsertRow(ServerContext* context, const csvservice::InsertRowRequest* request, csvservice::ModificationResponse* response) {
    const std::string& filename = request->filename();
    std::cout << "InsertRow called for file: " << filename << std::endl;

    // Define the handler function to perform the actual insertion
    auto handler = [this, &filename, request, response](const csvservice::InsertRowRequest*, csvservice::ModificationResponse* resp) -> grpc::Status {
        try {
            Mutation mutation;
            mutation.file = filename;
            RowInsert insert_op;
            // Convert protobuf repeated field to std::vector<std::string>
            insert_op.values.reserve(request->values_size());
            for (const auto& val : request->values()) {
                insert_op.values.push_back(val);
            }
            mutation.op = insert_op;

            state_->apply(mutation);

            resp->set_success(true);
            resp->set_message("Row inserted successfully.");
            std::cout << "Successfully inserted row into " << filename << std::endl;

            // Replicate the insert mutation to peers if this node is the leader
            if (registry_.is_leader()) {
                replicate_mutation_async(mutation);
            }

            return Status::OK;
        } catch (const std::exception& e) {
            resp->set_success(false);
            resp->set_message(std::string("Error inserting row: ") + e.what());
            std::cerr << "InsertRow exception for " << filename << ": " << e.what() << std::endl;
            return Status(grpc::StatusCode::INTERNAL, e.what());
        }
    };

    // Forward if needed, or execute locally if leader
    std::function<grpc::Status(const csvservice::InsertRowRequest*, csvservice::ModificationResponse*)> handler_func = handler;
    bool forwarded = forward_to_leader_if_needed(request, response, handler_func);
    if (forwarded) {
        // Response populated by forwarded call
        std::cout << "InsertRow request forwarded to leader for file: " << filename << std::endl;
        return response->success() ? Status::OK : Status(grpc::StatusCode::INTERNAL, response->message());
    } else {
        // Executed locally
        return handler(request, response);
    }
}

// ApplyMutation RPC handler - called by leader to replicate mutations to followers
Status CsvServiceImpl::ApplyMutation(ServerContext* context, const csvservice::ReplicateMutationRequest* request, csvservice::ReplicateMutationResponse* response) {
    // Extract the filename from the request
    const std::string& filename = request->filename();
    std::cout << "Received mutation from leader for file: " << filename << std::endl;

    try {
        // Convert the protobuf mutation to our internal Mutation type
        Mutation mutation;
        mutation.file = filename;
        
        // Handle different mutation types (insert or delete)
        if (request->has_row_insert()) {
            const auto& insert_mutation_proto = request->row_insert();
            // Convert proto repeated string to std::vector<std::string>
            mutation.op = RowInsert{std::vector<std::string>(
                insert_mutation_proto.values().begin(), 
                insert_mutation_proto.values().end())};
            std::cout << "Applying row insert mutation from leader" << std::endl;
        } 
        else if (request->has_row_delete()) {
            const auto& delete_mutation_proto = request->row_delete();
            mutation.op = RowDelete{delete_mutation_proto.row_index()};
            std::cout << "Applying row delete mutation from leader" << std::endl;
        } 
        else {
            // Invalid mutation type
            response->set_success(false);
            response->set_message("Invalid mutation type in request");
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid mutation type");
        }
        
        // Apply the mutation to our local state machine
        state_->apply(mutation);
        
        // Set success response
        response->set_success(true);
        response->set_message("Mutation successfully applied by follower");
        return Status::OK;
    } 
    catch (const std::exception& e) {
        std::cerr << "Error applying mutation from leader: " << e.what() << std::endl;
        response->set_success(false);
        response->set_message(std::string("Failed to apply mutation: ") + e.what());
        return Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

// Replicates a mutation from the leader to all followers asynchronously
void CsvServiceImpl::replicate_mutation_async(const Mutation& mutation) {
    // Only the leader should replicate mutations
    if (!registry_.is_leader()) {
        std::cerr << "Warning: Non-leader attempted to replicate mutation. Ignoring." << std::endl;
        return;
    }
    
    std::cout << "Leader replicating mutation for file: " << mutation.file << std::endl;
    
    // Get all servers in the cluster
    auto peers = registry_.get_all_servers();
    
    // For each peer (excluding self)
    for (const auto& peer_addr : peers) {
        // Skip self (leader doesn't need to replicate to itself)
        if (peer_addr == registry_.get_self_address()) {
            continue;
        }
        
        // Create a copy of the mutation for the thread
        Mutation mutation_copy = mutation;
        
        // Launch a detached thread for each follower to handle replication asynchronously
        std::thread([this, peer_addr, mutation_copy]() {
            std::cout << "Starting replication to follower: " << peer_addr << std::endl;
            
            // Retry parameters
            const int max_retries = 3;
            const auto retry_delay = std::chrono::milliseconds(500);
            
            // Create gRPC channel and stub
            auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
            auto stub = csvservice::CsvService::NewStub(channel);
            
            // Prepare the mutation request
            csvservice::ReplicateMutationRequest req;
            csvservice::ReplicateMutationResponse resp;
            
            // Set the filename in the request
            req.set_filename(mutation_copy.file);
            
            // Set the appropriate mutation type
            if (std::holds_alternative<RowInsert>(mutation_copy.op)) {
                // Handle row insert
                auto* row_insert_proto = req.mutable_row_insert();
                const auto& row_insert_data = std::get<RowInsert>(mutation_copy.op);
                // Add all values to the repeated field
                for (const auto& value : row_insert_data.values) {
                    row_insert_proto->add_values(value);
                }
            } 
            else if (std::holds_alternative<RowDelete>(mutation_copy.op)) {
                // Handle row delete
                auto* row_delete_proto = req.mutable_row_delete();
                row_delete_proto->set_row_index(std::get<RowDelete>(mutation_copy.op).row_index);
            }
            
            // Try to replicate with retries
            bool success = false;
            for (int attempt = 1; attempt <= max_retries && !success; ++attempt) {
                ClientContext context;
                
                // Set a reasonable deadline for the RPC
                std::chrono::system_clock::time_point deadline = 
                    std::chrono::system_clock::now() + std::chrono::seconds(5);
                context.set_deadline(deadline);
                
                // Make the RPC call
                Status status = stub->ApplyMutation(&context, req, &resp);
                
                if (status.ok() && resp.success()) {
                    // Successful replication
                    std::cout << "Successfully replicated mutation to " << peer_addr 
                              << " (attempt " << attempt << ")" << std::endl;
                    success = true;
                } 
                else {
                    // Failed replication
                    std::cerr << "Failed to replicate mutation to " << peer_addr 
                              << " (attempt " << attempt << "/" << max_retries << "): " 
                              << status.error_message() << std::endl;
                    
                    // If we have more retries, wait before trying again
                    if (attempt < max_retries) {
                        std::this_thread::sleep_for(retry_delay);
                    }
                }
            }
            
            if (!success) {
                std::cerr << "All replication attempts to " << peer_addr << " failed. "
                          << "Follower may be out of sync." << std::endl;
                
                // Note: In a more robust implementation, we might want to:
                // 1. Mark this follower as out of sync
                // 2. Implement a recovery mechanism for when it comes back online
                // 3. Consider removing it from the cluster if it's permanently down
            }
        }).detach();  // Detach the thread to let it run independently
    }
}

// Stub for ComputeSum
Status CsvServiceImpl::ComputeSum(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    std::cout << "ComputeSum called for file: " << filename << ", column: " << column_name << std::endl;
    
    // Get a view of the file data from the state machine
    TableView view = state_->view(filename);
    
    if (view.empty()) {
        response->set_success(false);
        response->set_message("File not found: " + filename);
        return Status::OK;
    }
    
    // Check if the column exists
    auto it = view.columns.find(column_name);
    if (it == view.columns.end()) {
        response->set_success(false);
        response->set_message("Column not found: " + column_name);
        return Status::OK;
    }
    
    // Calculate the sum
    double sum = 0.0;
    const auto& values = it->second;
    
    for (const auto& value_str : values) {
        try {
            // Convert string to double and add to sum
            double value = std::stod(value_str);
            sum += value;
        } catch (const std::exception& e) {
            // Skip non-numeric values
            std::cerr << "Warning: Non-numeric value in column " << column_name << ": " << value_str << std::endl;
        }
    }
    
    response->set_success(true);
    response->set_message("Sum calculated successfully");
    response->set_value(sum);
    
    return Status::OK;
}

// Stub for ComputeAverage
Status CsvServiceImpl::ComputeAverage(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    std::cout << "ComputeAverage called for file: " << filename << ", column: " << column_name << std::endl;
    
    // Get a view of the file data from the state machine
    TableView view = state_->view(filename);
    
    if (view.empty()) {
        response->set_success(false);
        response->set_message("File not found: " + filename);
        return Status::OK;
    }
    
    // Check if the column exists
    auto it = view.columns.find(column_name);
    if (it == view.columns.end()) {
        response->set_success(false);
        response->set_message("Column not found: " + column_name);
        return Status::OK;
    }
    
    // Calculate the average
    double sum = 0.0;
    const auto& values = it->second;
    int valid_count = 0;
    
    for (const auto& value_str : values) {
        try {
            // Convert string to double and add to sum
            double value = std::stod(value_str);
            sum += value;
            valid_count++;
        } catch (const std::exception& e) {
            // Skip non-numeric values
            std::cerr << "Warning: Non-numeric value in column " << column_name << ": " << value_str << std::endl;
        }
    }
    
    // Check if we have any valid values
    if (valid_count == 0) {
        response->set_success(false);
        response->set_message("No valid numeric values found in column: " + column_name);
        response->set_value(0.0);
        return Status::OK;
    }
    
    // Calculate average
    double average = sum / valid_count;
    
    response->set_success(true);
    response->set_message("Average calculated successfully");
    response->set_value(average);
    
    return Status::OK;
}

// Stub for ListLoadedFiles (RPC)
Status CsvServiceImpl::ListLoadedFiles(ServerContext* context, const csvservice::Empty* request, csvservice::CsvFileList* response) {
    std::cout << "[Stub] ListLoadedFiles RPC called." << std::endl;
    // TODO: Get file list from state machine
    const auto& files_map = get_loaded_files(); // Call the internal helper (returns map ref)
    for (const auto& file_pair : files_map) { // Iterate map pairs
        response->add_filenames(file_pair.first); // Add the filename (key) to the response
    }
    return Status::OK;
}

// Stub for RegisterPeer
Status CsvServiceImpl::RegisterPeer(ServerContext* context, const csvservice::RegisterPeerRequest* request, csvservice::RegisterPeerResponse* response) {
     // Use peer_address()
     std::cout << "[Stub] RegisterPeer called by: " << request->peer_address() << std::endl;
     // TODO: Implement actual peer registration logic in ServerRegistry if needed
     // registry_.register_peer(request->peer_address()); // Pass address
     response->set_success(true); // Assume success for stub
     response->set_message("Peer registered (stub).");
     return Status::OK;
}

// Stub for Heartbeat
Status CsvServiceImpl::Heartbeat(ServerContext* context, const csvservice::HeartbeatRequest* request, csvservice::HeartbeatResponse* response) {
    // std::cout << "[Stub] Heartbeat received from: " << request->sender_address() << std::endl; // sender_address may not exist
    // TODO: Update peer status in ServerRegistry based on heartbeat
    response->set_leader_address(registry_.get_leader()); // Respond with current leader
    // response->set_leader_term(0); // Incorrect field name
    response->set_success(true); // Set success field
    return Status::OK;
}

// Stub for GetClusterStatus
Status CsvServiceImpl::GetClusterStatus(ServerContext* context, const csvservice::Empty* request, csvservice::ClusterStatusResponse* response) {
     std::cout << "[Stub] GetClusterStatus called." << std::endl;
     response->set_leader_address(registry_.get_leader());
     // response->set_is_leader(registry_.is_leader()); // Incorrect field
     int count = 0;
     for (const auto& server : registry_.get_all_servers()) {
         response->add_server_addresses(server); // Use correct field name
         count++;
     }
     // Add self? ServerRegistry should probably handle this internally.
     // Let's assume get_all_servers() includes self if appropriate.
     // response->add_server_addresses(registry_.get_self_address());
     response->set_active_server_count(count); // Use correct field name
     response->set_success(true);
     response->set_message("Cluster status fetched (stub)."); // Added semicolon
     return Status::OK;
}

// Stub for get_loaded_files (Internal helper used by menu.cpp)
// Match return type from header: const std::unordered_map<std::string, ColumnStore>&
const std::unordered_map<std::string, ColumnStore>& CsvServiceImpl::get_loaded_files() const {
    std::cout << "get_loaded_files called." << std::endl;
    
    auto* mem_state = dynamic_cast<const InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        // Use the get_files() method from InMemoryStateMachine to get the actual files
        return mem_state->get_files();
    }
    
    // Return static empty map if state machine not available
    static const std::unordered_map<std::string, ColumnStore> empty_map_fallback;
    return empty_map_fallback;
}


} // namespace network
