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
    grpc::ServerContext* context,
    const csvservice::CsvUploadRequest* request,
    csvservice::CsvUploadResponse* response) {
    
    // Get leader and self addresses
    std::string leader_address = registry_.get_leader();
    std::string self_addr = registry_.get_self_address();
    
    std::cout << "UploadCsv called for file: " << request->filename() 
              << " | Self: " << self_addr 
              << " | Leader: " << leader_address 
              << " | Is leader: " << (registry_.is_leader() ? "Yes" : "No") << std::endl;
    
    // List all known servers for debugging
    auto all_servers = registry_.get_all_servers();
    std::cout << "Known servers in cluster (" << all_servers.size() << "): ";
    for (const auto& server : all_servers) {
        std::cout << server << " ";
    }
    std::cout << std::endl;
    
    // If this server is the leader, ensure it knows about all other servers in the cluster
    if (registry_.is_leader()) {
        std::cout << "Leader server checking for all known peers..." << std::endl;
        
        // Hardcoded list of expected servers in our cluster
        std::vector<std::string> expected_servers = {
            "localhost:50051", 
            "localhost:50052", 
            "localhost:50053"
        };
        
        // Check which servers are missing from our known servers list
        std::vector<std::string> missing_servers;
        for (const auto& expected : expected_servers) {
            if (expected != self_addr && 
                std::find(all_servers.begin(), all_servers.end(), expected) == all_servers.end()) {
                missing_servers.push_back(expected);
            }
        }
        
        // Try to register with any missing servers
        if (!missing_servers.empty()) {
            std::cout << "WARNING: Missing " << missing_servers.size() 
                      << " expected servers. Attempting to register with them..." << std::endl;
            
            for (const auto& peer : missing_servers) {
                std::cout << "Attempting to register with peer: " << peer << std::endl;
                
                // Create a channel to the peer
                auto channel_options = grpc::ChannelArguments();
                channel_options.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 100 * 1024 * 1024); // 100MB
                channel_options.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 100 * 1024 * 1024); // 100MB
                
                auto channel = grpc::CreateCustomChannel(
                    peer, 
                    grpc::InsecureChannelCredentials(),
                    channel_options
                );
                
                auto stub = csvservice::CsvService::NewStub(channel);
                
                // Create a registration request
                csvservice::RegisterPeerRequest reg_request;
                reg_request.set_peer_address(self_addr);
                
                csvservice::RegisterPeerResponse reg_response;
                ClientContext client_context;
                
                // Set a timeout for the registration
                std::chrono::system_clock::time_point deadline = 
                    std::chrono::system_clock::now() + std::chrono::seconds(3);
                client_context.set_deadline(deadline);
                
                // Try to register with the peer
                Status status = stub->RegisterPeer(&client_context, reg_request, &reg_response);
                
                if (status.ok()) {
                    std::cout << "Successfully registered with peer: " << peer << std::endl;
                    registry_.register_peer(peer);
                } else {
                    std::cerr << "Failed to register with peer " << peer << ": " 
                              << status.error_code() << ": " << status.error_message() << std::endl;
                }
            }
            
            // Refresh the list of servers after registration attempts
            all_servers = registry_.get_all_servers();
            std::cout << "Updated known servers in cluster (" << all_servers.size() << "): ";
            for (const auto& server : all_servers) {
                std::cout << server << " ";
            }
            std::cout << std::endl;
        }
    }
    
    if (!leader_address.empty() && leader_address != self_addr) { 
        // --- Follower: Forward to Leader --- 
        std::cout << "Forwarding UploadCsv for " << request->filename() << " to leader: " << leader_address << std::endl; 
        
        // Create channel with proper message size limits
        auto channel_options = grpc::ChannelArguments();
        channel_options.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 100 * 1024 * 1024); // 100MB
        channel_options.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 100 * 1024 * 1024); // 100MB
        
        auto channel = grpc::CreateCustomChannel(
            leader_address, 
            grpc::InsecureChannelCredentials(),
            channel_options
        );
        
        auto stub = csvservice::CsvService::NewStub(channel);
        
        grpc::ClientContext client_context;
        // Set a reasonable deadline for the forwarded call
        std::chrono::system_clock::time_point deadline = 
            std::chrono::system_clock::now() + std::chrono::seconds(30); // Increased timeout for majority ack
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
            std::cout << "Successfully added file " << filename << " to local state machine." << std::endl;
        } else { 
            throw std::runtime_error("Internal error: State machine not available"); 
        } 
         
        // Set initial success response (will be updated based on replication result)
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
     
    // 2. Replicate to Peers with Majority Acknowledgment (if this is the leader in a cluster)
    if (!leader_address.empty() && leader_address == self_addr) { // Double check we are leader
        std::cout << "Replicating " << filename << " to peers with majority acknowledgment..." << std::endl;
        
        // Use the new majority acknowledgment method
        bool majority_ack = replicate_upload_with_majority_ack(*request);
        
        if (majority_ack) {
            std::cout << "Successfully replicated " << filename << " to majority of peers." << std::endl;
            response->set_message("CSV file loaded successfully and replicated to majority of peers.");
        } else {
            std::cerr << "Failed to replicate " << filename << " to majority of peers." << std::endl;
            response->set_success(false);
            response->set_message("CSV file loaded locally but failed to replicate to majority of peers. Please retry.");
            return Status(grpc::StatusCode::INTERNAL, "Failed to replicate to majority of peers");
        }
    }

    // Return OK status to the original client after local processing and replication
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
    
    // Verify we received the data
    std::cout << "Replication data received: filename=" << filename 
              << ", data size=" << csv_data.size() << " bytes" << std::endl;
    
    if (csv_data.empty()) {
        std::cerr << "ERROR: Empty CSV data received for file: " << filename << std::endl;
        response->set_success(false);
        response->set_message("Empty CSV data received");
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Empty CSV data");
    }
    
    try { 
        // Parse the CSV data
        std::cout << "Parsing CSV data for file: " << filename << std::endl;
        ColumnStore column_store = csv::parse_csv(csv_data); 
        
        int row_count = column_store.columns.empty() ? 0 : column_store.columns.begin()->second.size();
        int col_count = column_store.column_names.size();
        std::cout << "Successfully parsed CSV data: " << row_count << " rows, " 
                  << col_count << " columns" << std::endl;
        
        // Get access to the state machine
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get()); 
        if (mem_state) { 
            // Add the file to the local state machine
            std::cout << "Adding file to local state machine: " << filename << std::endl;
            
            // Check if file already exists
            if (mem_state->file_exists(filename)) {
                std::cout << "WARNING: File already exists in state machine. Overwriting: " << filename << std::endl;
            }
            
            // Add or update the file in the state machine
            mem_state->add_csv_file(filename, column_store.column_names, column_store.columns); 
            
            // Verify the file was added successfully
            if (mem_state->file_exists(filename)) {
                std::cout << "Verified file was successfully added to state machine: " << filename << std::endl;
                
                // Get the file to double-check
                try {
                    TableView view = mem_state->view(filename);
                    std::cout << "Successfully verified file in state machine: " 
                              << filename << " with " 
                              << view.row_count() << " rows and "
                              << view.column_names.size() << " columns" << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "WARNING: File was added but verification failed: " << e.what() << std::endl;
                }
            } else {
                std::cerr << "ERROR: File was not successfully added to state machine: " << filename << std::endl;
                response->set_success(false);
                response->set_message("File was not successfully added to state machine");
                return Status(grpc::StatusCode::INTERNAL, "File addition verification failed");
            }
            
            // Set success response
            response->set_success(true); 
            response->set_message("File successfully replicated to follower");
            std::cout << "Successfully replicated file: " << filename << " from leader" << std::endl;
            return Status::OK;
        } else { 
            // State machine not available
            std::cerr << "ERROR: State machine unavailable for file: " << filename << std::endl;
            response->set_success(false); 
            response->set_message("Internal error: State machine not available"); 
            std::cerr << "Failed to replicate file: State machine unavailable" << std::endl;
            return Status(grpc::StatusCode::INTERNAL, "State machine unavailable");
        } 
    } catch (const std::exception& e) { 
        // Error processing the file
        std::cerr << "ERROR: Exception while replicating file " << filename << ": " << e.what() << std::endl;
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
    auto handler = [this, &filename, row_index](const csvservice::DeleteRowRequest* req, csvservice::ModificationResponse* resp) -> grpc::Status {
        try {
            // Create mutation object
            Mutation mutation;
            mutation.file = filename;
            mutation.op = RowDelete{row_index};
            
            // Apply the mutation locally
            state_->apply(mutation);

            // Set initial success response
            resp->set_success(true);
            resp->set_message("Row deleted successfully.");
            std::cout << "Successfully deleted row " << row_index << " from " << filename << std::endl;
            
            // Replicate the delete mutation to peers with majority acknowledgment if this node is the leader
            if (registry_.is_leader()) {
                std::cout << "Replicating row deletion to peers with majority acknowledgment..." << std::endl;
                
                // Use the majority acknowledgment method
                bool majority_ack = replicate_with_majority_ack(mutation);
                
                if (majority_ack) {
                    std::cout << "Successfully replicated row deletion to majority of peers." << std::endl;
                    resp->set_message("Row deleted successfully and replicated to majority of peers.");
                } else {
                    std::cerr << "Failed to replicate row deletion to majority of peers." << std::endl;
                    resp->set_success(false);
                    resp->set_message("Row deleted locally but failed to replicate to majority of peers. Please retry.");
                    return Status(grpc::StatusCode::INTERNAL, "Failed to replicate to majority of peers");
                }
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
            // Create mutation object
            Mutation mutation;
            mutation.file = filename;
            RowInsert insert_op;
            
            // Convert protobuf repeated field to std::vector<std::string>
            insert_op.values.reserve(request->values_size());
            for (const auto& val : request->values()) {
                insert_op.values.push_back(val);
            }
            mutation.op = insert_op;

            // Apply the mutation locally
            state_->apply(mutation);

            // Set initial success response
            resp->set_success(true);
            resp->set_message("Row inserted successfully.");
            std::cout << "Successfully inserted row into " << filename << std::endl;

            // Replicate the insert mutation to peers with majority acknowledgment if this node is the leader
            if (registry_.is_leader()) {
                std::cout << "Replicating row insertion to peers with majority acknowledgment..." << std::endl;
                
                // Use the majority acknowledgment method
                bool majority_ack = replicate_with_majority_ack(mutation);
                
                if (majority_ack) {
                    std::cout << "Successfully replicated row insertion to majority of peers." << std::endl;
                    resp->set_message("Row inserted successfully and replicated to majority of peers.");
                } else {
                    std::cerr << "Failed to replicate row insertion to majority of peers." << std::endl;
                    resp->set_success(false);
                    resp->set_message("Row inserted locally but failed to replicate to majority of peers. Please retry.");
                    return Status(grpc::StatusCode::INTERNAL, "Failed to replicate to majority of peers");
                }
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
        // First, check if the file exists in our state machine
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
        if (!mem_state) {
            std::cerr << "ERROR: State machine unavailable for mutation on file: " << filename << std::endl;
            response->set_success(false);
            response->set_message("Internal error: State machine not available");
            return Status(grpc::StatusCode::INTERNAL, "State machine unavailable");
        }
        
        // Check if the file exists before applying the mutation
        if (!mem_state->file_exists(filename)) {
            std::cerr << "ERROR: Cannot apply mutation to non-existent file: " << filename << std::endl;
            response->set_success(false);
            response->set_message("File does not exist in state machine");
            return Status(grpc::StatusCode::NOT_FOUND, "File not found");
        }
        
        // Convert the protobuf mutation to our internal Mutation type
        Mutation mutation;
        mutation.file = filename;
        
        // Handle different mutation types (insert or delete)
        if (request->has_row_insert()) {
            // Handle row insert
            const auto& row_insert_proto = request->row_insert();
            // Convert proto repeated string to std::vector<std::string>
            std::vector<std::string> values(
                row_insert_proto.values().begin(), 
                row_insert_proto.values().end());
                
            mutation.op = RowInsert{values};
            
            std::cout << "Applying row insert mutation from leader with " 
                      << values.size() << " values" << std::endl;
                      
            // Log the values being inserted for debugging
            std::cout << "Values to insert: ";
            for (const auto& value : values) {
                std::cout << "'" << value << "' ";
            }
            std::cout << std::endl;
        } 
        else if (request->has_row_delete()) {
            // Handle row delete
            const auto& row_delete_proto = request->row_delete();
            int row_index = row_delete_proto.row_index();
            mutation.op = RowDelete{row_index};
            std::cout << "Applying row delete mutation from leader for row index: " 
                      << row_index << std::endl;
        } 
        else {
            // Invalid mutation type
            std::cerr << "ERROR: Invalid mutation type in request for file: " << filename << std::endl;
            response->set_success(false);
            response->set_message("Invalid mutation type in request");
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid mutation type");
        }
        
        // Get the state before applying the mutation
        int row_count_before = 0;
        try {
            TableView view = mem_state->view(filename);
            row_count_before = view.row_count();
            std::cout << "Current state before mutation: " << filename 
                      << " has " << row_count_before << " rows" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "WARNING: Could not get state before mutation: " << e.what() << std::endl;
        }
        
        // Apply the mutation to our local state machine
        std::cout << "Applying mutation to local state machine..." << std::endl;
        state_->apply(mutation);
        
        // Verify the mutation was applied successfully
        try {
            TableView view = mem_state->view(filename);
            int row_count_after = view.row_count();
            std::cout << "Mutation applied successfully. " << filename 
                      << " now has " << row_count_after << " rows (was " 
                      << row_count_before << " rows)" << std::endl;
                      
            // For row insert, we expect row count to increase by 1
            if (mutation.has_insert() && row_count_after != row_count_before + 1) {
                std::cerr << "WARNING: After row insert, row count did not increase by 1" << std::endl;
            }
            
            // For row delete, we expect row count to decrease by 1
            if (mutation.has_delete() && row_count_after != row_count_before - 1) {
                std::cerr << "WARNING: After row delete, row count did not decrease by 1" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "WARNING: Could not verify mutation result: " << e.what() << std::endl;
        }
        
        // Set success response
        response->set_success(true);
        response->set_message("Mutation successfully applied by follower");
        return Status::OK;
    } 
    catch (const std::exception& e) {
        std::cerr << "ERROR: Exception while applying mutation from leader: " << e.what() << std::endl;
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

// Replicates a mutation to all followers and waits for majority acknowledgment
bool CsvServiceImpl::replicate_with_majority_ack(const Mutation& mutation) {
    // Only the leader should replicate mutations
    if (!registry_.is_leader()) {
        std::cerr << "Warning: Non-leader attempted to replicate with majority ack. Ignoring." << std::endl;
        return false;
    }
    
    std::cout << "Leader replicating mutation for file: " << mutation.file << " with majority acknowledgment" << std::endl;
    
    // Get all servers in the cluster
    auto peers = registry_.get_all_servers();
    
    // Calculate the number of servers needed for majority acknowledgment
    // In a cluster of n servers, majority is (n/2)+1
    int total_servers = peers.size();
    int majority_threshold = (total_servers / 2) + 1;
    
    std::cout << "Total servers: " << total_servers << ", Majority threshold: " << majority_threshold << std::endl;
    std::cout << "All servers in cluster: ";
    for (const auto& server : peers) {
        std::cout << server << " ";
    }
    std::cout << std::endl;
    
    // Count self (leader) as already acknowledged
    int ack_count = 1;
    
    // Create vectors to track which servers have acknowledged and which failed
    std::vector<std::string> acknowledged_servers = {registry_.get_self_address()};
    std::vector<std::string> failed_servers;
    
    // Create the mutation request
    csvservice::ReplicateMutationRequest request;
    request.set_filename(mutation.file);
    
    // Set the appropriate mutation type based on the variant
    if (mutation.has_insert()) {
        // Handle row insert
        auto* row_insert_proto = request.mutable_row_insert();
        const auto& row_insert_data = mutation.insert();
        // Add all values to the repeated field
        for (const auto& value : row_insert_data.values) {
            row_insert_proto->add_values(value);
        }
    } 
    else if (mutation.has_delete()) {
        // Handle row delete
        auto* row_delete_proto = request.mutable_row_delete();
        row_delete_proto->set_row_index(mutation.del().row_index);
    }
    
    std::cout << "Starting replication of mutation on " << mutation.file 
              << " to " << (peers.size() - 1) << " followers" << std::endl;
    
    // For each peer (excluding self), attempt to replicate synchronously
    for (const auto& peer_addr : peers) {
        // Skip self (leader doesn't need to replicate to itself)
        if (peer_addr == registry_.get_self_address()) {
            continue;
        }
        
        std::cout << "Starting mutation replication to follower: " << peer_addr << std::endl;
        
        // Retry parameters
        const int max_retries = 5;
        const auto retry_delay = std::chrono::milliseconds(300);
        
        // Create gRPC channel and stub
        auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
        auto stub = csvservice::CsvService::NewStub(channel);
        
        // Try to replicate with retries
        bool success = false;
        for (int attempt = 1; attempt <= max_retries && !success; ++attempt) {
            ClientContext context;
            
            // Set a reasonable deadline for the RPC
            std::chrono::system_clock::time_point deadline = 
                std::chrono::system_clock::now() + std::chrono::seconds(5);
            context.set_deadline(deadline);
            
            std::cout << "Replication attempt " << attempt << " for mutation on " << mutation.file 
                      << " to server " << peer_addr << std::endl;
            
            // Make the RPC call
            csvservice::ReplicateMutationResponse resp;
            Status status = stub->ApplyMutation(&context, request, &resp);
            
            if (status.ok() && resp.success()) {
                // Successful replication
                std::cout << "Successfully replicated mutation to " << peer_addr 
                          << " (attempt " << attempt << ")" << std::endl;
                success = true;
                
                // Update acknowledgment tracking
                ack_count++;
                acknowledged_servers.push_back(peer_addr);
                
                std::cout << "Current ack count: " << ack_count << "/" << majority_threshold 
                          << " (from: " << peer_addr << ")" << std::endl;
                
                // Check if we've reached majority (but continue replicating to all)
                if (ack_count == majority_threshold) {
                    std::cout << "Reached majority acknowledgment with " << peer_addr 
                              << ", but continuing to replicate to all servers" << std::endl;
                }
            } 
            else {
                // Failed replication
                std::cerr << "Failed to replicate mutation to " << peer_addr 
                          << " (attempt " << attempt << "/" << max_retries << "): " 
                          << status.error_code() << ": " << status.error_message();
                
                if (status.ok() && !resp.success()) {
                    std::cerr << " (Server error: " << resp.message() << ")";
                }
                std::cerr << std::endl;
                
                // If we have more retries, wait before trying again
                if (attempt < max_retries) {
                    std::cout << "Waiting " << retry_delay.count() << "ms before retry..." << std::endl;
                    std::this_thread::sleep_for(retry_delay);
                }
            }
        }
        
        // If all retries failed, add to failed servers list
        if (!success) {
            failed_servers.push_back(peer_addr);
            std::cerr << "All replication attempts to " << peer_addr << " failed. "
                      << "Adding to recovery queue for background replication." << std::endl;
        }
    }
    
    // Log final replication status
    std::cout << "Replication complete. " << acknowledged_servers.size() << "/" << total_servers 
              << " servers acknowledged the mutation" << std::endl;
    
    std::cout << "Acknowledged servers: ";
    for (const auto& server : acknowledged_servers) {
        std::cout << server << " ";
    }
    std::cout << std::endl;
    
    if (!failed_servers.empty()) {
        std::cout << "Failed servers: ";
        for (const auto& server : failed_servers) {
            std::cout << server << " ";
        }
        std::cout << std::endl;
        
        // Start background recovery for any failed servers
        start_background_recovery(mutation, failed_servers);
    }
    
    // Return true if we reached majority, false otherwise
    return (ack_count >= majority_threshold);
}

// Replicates a file upload to all followers and waits for majority acknowledgment
bool CsvServiceImpl::replicate_upload_with_majority_ack(const csvservice::CsvUploadRequest& request) {
    // Only the leader should replicate uploads
    if (!registry_.is_leader()) {
        std::cerr << "Warning: Non-leader attempted to replicate upload with majority ack. Ignoring." << std::endl;
        return false;
    }
    
    std::string filename = request.filename();
    std::cout << "Leader replicating file upload for: " << filename << " with majority acknowledgment" << std::endl;
    
    // Get all servers in the cluster
    auto peers = registry_.get_all_servers();
    
    // Calculate the number of servers needed for majority acknowledgment
    // In a cluster of n servers, majority is (n/2)+1
    int total_servers = peers.size();
    int majority_threshold = (total_servers / 2) + 1;
    
    std::cout << "Total servers: " << total_servers << ", Majority threshold: " << majority_threshold << std::endl;
    std::cout << "All servers in cluster: ";
    for (const auto& server : peers) {
        std::cout << server << " ";
    }
    std::cout << std::endl;
    
    // Count self (leader) as already acknowledged
    int ack_count = 1;
    
    // Create vectors to track which servers have acknowledged and which failed
    std::vector<std::string> acknowledged_servers = {registry_.get_self_address()};
    std::vector<std::string> failed_servers;
    
    // Verify the data is not empty
    if (request.csv_data().empty()) {
        std::cerr << "ERROR: Attempting to replicate empty CSV data for file: " << filename << std::endl;
        return false;
    }
    
    std::cout << "Starting replication of " << filename << " (" << request.csv_data().size() 
              << " bytes) to " << (peers.size() - 1) << " followers" << std::endl;
    
    // For each peer (excluding self), attempt to replicate synchronously
    for (const auto& peer_addr : peers) {
        // Skip self (leader doesn't need to replicate to itself)
        if (peer_addr == registry_.get_self_address()) {
            continue;
        }
        
        std::cout << "Starting file upload replication to follower: " << peer_addr << std::endl;
        
        // Retry parameters
        const int max_retries = 5;
        const auto retry_delay = std::chrono::milliseconds(300);
        
        // Create gRPC channel and stub with longer timeout
        auto channel_options = grpc::ChannelArguments();
        channel_options.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 100 * 1024 * 1024); // 100MB
        channel_options.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 100 * 1024 * 1024); // 100MB
        
        auto channel = grpc::CreateCustomChannel(
            peer_addr, 
            grpc::InsecureChannelCredentials(),
            channel_options
        );
        
        auto stub = csvservice::CsvService::NewStub(channel);
        
        // Try to replicate with retries
        bool success = false;
        for (int attempt = 1; attempt <= max_retries && !success; ++attempt) {
            ClientContext context;
            
            // Set a reasonable deadline for the RPC
            std::chrono::system_clock::time_point deadline = 
                std::chrono::system_clock::now() + std::chrono::seconds(10);
            context.set_deadline(deadline);
            
            std::cout << "Replication attempt " << attempt << " for file upload " << filename 
                      << " to server " << peer_addr << std::endl;
            
            // Make the RPC call
            csvservice::ReplicateUploadResponse resp;
            Status status = stub->ReplicateUpload(&context, request, &resp);
            
            if (status.ok() && resp.success()) {
                // Successful replication
                std::cout << "Successfully replicated file upload to " << peer_addr 
                          << " (attempt " << attempt << ")" << std::endl;
                success = true;
                
                // Update acknowledgment tracking
                ack_count++;
                acknowledged_servers.push_back(peer_addr);
                
                std::cout << "Current ack count: " << ack_count << "/" << majority_threshold 
                          << " (from: " << peer_addr << ")" << std::endl;
                
                // Check if we've reached majority (but continue replicating to all)
                if (ack_count == majority_threshold) {
                    std::cout << "Reached majority acknowledgment with " << peer_addr 
                              << ", but continuing to replicate to all servers" << std::endl;
                }
            } 
            else {
                // Failed replication
                std::cerr << "Failed to replicate file upload to " << peer_addr 
                          << " (attempt " << attempt << "/" << max_retries << "): " 
                          << status.error_code() << ": " << status.error_message();
                
                if (status.ok() && !resp.success()) {
                    std::cerr << " (Server error: " << resp.message() << ")";
                }
                std::cerr << std::endl;
                
                // If we have more retries, wait before trying again
                if (attempt < max_retries) {
                    std::cout << "Waiting " << retry_delay.count() << "ms before retry..." << std::endl;
                    std::this_thread::sleep_for(retry_delay);
                }
            }
        }
        
        // If all retries failed, add to failed servers list
        if (!success) {
            failed_servers.push_back(peer_addr);
            std::cerr << "All replication attempts to " << peer_addr << " failed. "
                      << "Adding to recovery queue for background replication." << std::endl;
        }
    }
    
    // Log final replication status
    std::cout << "Replication complete. " << acknowledged_servers.size() << "/" << total_servers 
              << " servers acknowledged the file upload" << std::endl;
    
    std::cout << "Acknowledged servers: ";
    for (const auto& server : acknowledged_servers) {
        std::cout << server << " ";
    }
    std::cout << std::endl;
    
    if (!failed_servers.empty()) {
        std::cout << "Failed servers: ";
        for (const auto& server : failed_servers) {
            std::cout << server << " ";
        }
        std::cout << std::endl;
        
        // Start background recovery for any failed servers
        start_background_upload_recovery(request, failed_servers);
    }
    
    // Return true if we reached majority, false otherwise
    return (ack_count >= majority_threshold);
}

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

Status CsvServiceImpl::ListLoadedFiles(ServerContext* context, const csvservice::Empty* request, csvservice::CsvFileList* response) {
    std::cout << "[Stub] ListLoadedFiles RPC called." << std::endl;
    // TODO: Get file list from state machine
    const auto& files_map = get_loaded_files(); // Call the internal helper (returns map ref)
    for (const auto& file_pair : files_map) { // Iterate map pairs
        response->add_filenames(file_pair.first); // Add the filename (key) to the response
    }
    return Status::OK;
}

Status CsvServiceImpl::GetClusterStatus(ServerContext* context, const csvservice::Empty* request, csvservice::ClusterStatusResponse* response) {
    std::cout << "[Stub] GetClusterStatus called." << std::endl;
    
    // Set the leader address in the response
    response->set_leader_address(registry_.get_leader());
    
    // Get all servers and add them to the response
    auto all_servers = registry_.get_all_servers();
    for (const auto& server : all_servers) {
        response->add_server_addresses(server);
    }
    
    // Set the active server count
    response->set_active_server_count(all_servers.size());
    
    // Set success status
    response->set_success(true);
    response->set_message("Cluster status fetched successfully");
    
    return Status::OK;
}

Status CsvServiceImpl::RegisterPeer(ServerContext* context, const csvservice::RegisterPeerRequest* request, csvservice::RegisterPeerResponse* response) {
    // Extract the peer address from the request
    std::string peer_address = request->peer_address();
    std::cout << "[Stub] RegisterPeer called by: " << peer_address << std::endl;
    
    // Register the peer with our registry
    registry_.register_peer(peer_address);
    
    // Set the leader address in the response
    response->set_leader_address(registry_.get_leader());
    
    // Set success status
    response->set_success(true);
    response->set_message("Peer registration successful from " + registry_.get_self_address());
    
    // Also try to register ourselves with the peer (bidirectional registration)
    if (peer_address != registry_.get_self_address()) {
        std::cout << "Attempting bidirectional registration with peer: " << peer_address << std::endl;
        
        // Create a channel to the peer
        auto channel = grpc::CreateChannel(peer_address, grpc::InsecureChannelCredentials());
        auto stub = csvservice::CsvService::NewStub(channel);
        
        // Create a registration request
        csvservice::RegisterPeerRequest reg_request;
        reg_request.set_peer_address(registry_.get_self_address());
        
        csvservice::RegisterPeerResponse reg_response;
        ClientContext client_context;
        
        // Set a timeout for the registration
        std::chrono::system_clock::time_point deadline = 
            std::chrono::system_clock::now() + std::chrono::seconds(2);
        client_context.set_deadline(deadline);
        
        // Try to register with the peer
        Status status = stub->RegisterPeer(&client_context, reg_request, &reg_response);
        
        if (status.ok()) {
            std::cout << "Successfully completed bidirectional registration with peer: " << peer_address << std::endl;
        } else {
            std::cerr << "Failed to complete bidirectional registration with peer " << peer_address << ": " 
                      << status.error_code() << ": " << status.error_message() << std::endl;
        }
    }
    
    return Status::OK;
}

Status CsvServiceImpl::Heartbeat(ServerContext* context, const csvservice::HeartbeatRequest* request, csvservice::HeartbeatResponse* response) {
    // std::cout << "[Stub] Heartbeat received from: " << request->sender_address() << std::endl; // sender_address may not exist
    // TODO: Update peer status in ServerRegistry based on heartbeat
    response->set_leader_address(registry_.get_leader()); // Respond with current leader
    // response->set_leader_term(0); // Incorrect field name
    response->set_success(true); // Set success field
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

// Background recovery for mutations to ensure eventual consistency across all servers
void CsvServiceImpl::start_background_recovery(const Mutation& mutation, const std::vector<std::string>& failed_servers) {
    // Create a thread to handle background recovery
    std::thread([this, mutation, failed_servers]() {
        std::cout << "Starting background recovery for mutation on file: " << mutation.file 
                  << " for " << failed_servers.size() << " failed servers" << std::endl;
        
        // Recovery parameters - more aggressive to ensure eventual consistency
        const int max_recovery_retries = 20; // Increased from 10 to 20
        const auto initial_retry_delay = std::chrono::milliseconds(500); // Reduced initial delay
        const auto max_retry_delay = std::chrono::milliseconds(10000); // 10 seconds in milliseconds
        
        // For each failed server
        for (const auto& peer_addr : failed_servers) {
            // Create a copy of the mutation for this server
            Mutation mutation_copy = mutation;
            
            // Launch a thread for each failed server to handle recovery
            std::thread([this, peer_addr, mutation_copy, max_recovery_retries, initial_retry_delay, max_retry_delay]() {
                std::cout << "Starting background recovery for server: " << peer_addr << std::endl;
                
                // Create gRPC channel and stub
                auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
                auto stub = csvservice::CsvService::NewStub(channel);
                
                // Prepare the mutation request
                csvservice::ReplicateMutationRequest req;
                csvservice::ReplicateMutationResponse resp;
                
                // Set the filename in the request
                req.set_filename(mutation_copy.file);
                
                // Set the appropriate mutation type
                if (mutation_copy.has_insert()) {
                    // Handle row insert
                    auto* row_insert_proto = req.mutable_row_insert();
                    const auto& row_insert_data = mutation_copy.insert();
                    // Add all values to the repeated field
                    for (const auto& value : row_insert_data.values) {
                        row_insert_proto->add_values(value);
                    }
                } 
                else if (mutation_copy.has_delete()) {
                    // Handle row delete
                    auto* row_delete_proto = req.mutable_row_delete();
                    row_delete_proto->set_row_index(mutation_copy.del().row_index);
                }
                
                // Try to recover with exponential backoff
                bool success = false;
                auto current_delay = initial_retry_delay;
                
                for (int attempt = 1; attempt <= max_recovery_retries && !success; ++attempt) {
                    ClientContext context;
                    
                    // Set a reasonable deadline for the RPC
                    std::chrono::system_clock::time_point deadline = 
                        std::chrono::system_clock::now() + std::chrono::seconds(5);
                    context.set_deadline(deadline);
                    
                    std::cout << "Recovery attempt " << attempt << " for mutation on " << mutation_copy.file 
                              << " to server " << peer_addr << std::endl;
                    
                    // Make the RPC call
                    Status status = stub->ApplyMutation(&context, req, &resp);
                    
                    if (status.ok() && resp.success()) {
                        // Successful recovery
                        std::cout << "Successfully recovered mutation for " << peer_addr 
                                  << " (attempt " << attempt << ")" << std::endl;
                        success = true;
                    } 
                    else {
                        // Failed recovery
                        std::cerr << "Failed to recover mutation for " << peer_addr 
                                  << " (attempt " << attempt << "/" << max_recovery_retries << "): " 
                                  << status.error_message() << std::endl;
                        
                        // If we have more retries, wait with exponential backoff before trying again
                        if (attempt < max_recovery_retries) {
                            std::cout << "Waiting " << current_delay.count() << "ms before next recovery attempt" << std::endl;
                            std::this_thread::sleep_for(current_delay);
                            // Exponential backoff with a maximum delay (using same duration type)
                            current_delay = std::min(current_delay * 2, max_retry_delay);
                        }
                    }
                }
                
                if (success) {
                    std::cout << "Background recovery for " << peer_addr << " completed successfully." << std::endl;
                } else {
                    std::cerr << "Background recovery for " << peer_addr << " failed after " 
                              << max_recovery_retries << " attempts. Server may remain out of sync." << std::endl;
                }
            }).detach();  // Detach the thread to let it run independently
        }
    }).detach();  // Detach the main recovery thread
}

// Background recovery for file uploads to ensure eventual consistency across all servers
void CsvServiceImpl::start_background_upload_recovery(const csvservice::CsvUploadRequest& request, 
                                                     const std::vector<std::string>& failed_servers) {
    // Create a thread to handle background recovery
    std::thread([this, request, failed_servers]() {
        std::cout << "Starting background recovery for file upload: " << request.filename() 
                  << " for " << failed_servers.size() << " failed servers" << std::endl;
        
        // Recovery parameters - more aggressive to ensure eventual consistency
        const int max_recovery_retries = 20; // Increased from 10 to 20
        const auto initial_retry_delay = std::chrono::milliseconds(500); // Reduced initial delay
        const auto max_retry_delay = std::chrono::milliseconds(10000); // 10 seconds in milliseconds
        
        // Create a shared_ptr to manage the request's lifetime across threads
        auto request_copy_ptr = std::make_shared<csvservice::CsvUploadRequest>(request);
        
        // For each failed server
        for (const auto& peer_addr : failed_servers) {
            // Launch a thread for each failed server to handle recovery
            std::thread([peer_addr, request_copy_ptr, max_recovery_retries, initial_retry_delay, max_retry_delay]() {
                std::cout << "Starting background upload recovery for server: " << peer_addr << std::endl;
                
                // Create gRPC channel and stub
                auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
                auto stub = csvservice::CsvService::NewStub(channel);
                
                // Prepare the replication response
                csvservice::ReplicateUploadResponse resp;
                
                // Try to recover with exponential backoff
                bool success = false;
                auto current_delay = initial_retry_delay;
                
                for (int attempt = 1; attempt <= max_recovery_retries && !success; ++attempt) {
                    ClientContext context;
                    
                    // Set a reasonable deadline for the RPC
                    std::chrono::system_clock::time_point deadline = 
                        std::chrono::system_clock::now() + std::chrono::seconds(10);
                    context.set_deadline(deadline);
                    
                    std::cout << "Recovery attempt " << attempt << " for file upload " << request_copy_ptr->filename() 
                              << " to server " << peer_addr << std::endl;
                    
                    // Make the RPC call
                    Status status = stub->ReplicateUpload(&context, *request_copy_ptr, &resp);
                    
                    if (status.ok() && resp.success()) {
                        // Successful recovery
                        std::cout << "Successfully recovered file upload for " << peer_addr 
                                  << " (attempt " << attempt << ")" << std::endl;
                        success = true;
                    } 
                    else {
                        // Failed recovery
                        std::cerr << "Failed to recover file upload for " << peer_addr 
                                  << " (attempt " << attempt << "/" << max_recovery_retries << "): " 
                                  << status.error_message() << std::endl;
                        
                        // If we have more retries, wait with exponential backoff before trying again
                        if (attempt < max_recovery_retries) {
                            std::cout << "Waiting " << current_delay.count() << "ms before next recovery attempt" << std::endl;
                            std::this_thread::sleep_for(current_delay);
                            // Exponential backoff with a maximum delay (using same duration type)
                            current_delay = std::min(current_delay * 2, max_retry_delay);
                        }
                    }
                }
                
                if (success) {
                    std::cout << "Background upload recovery for " << peer_addr << " completed successfully." << std::endl;
                } else {
                    std::cerr << "Background upload recovery for " << peer_addr << " failed after " 
                              << max_recovery_retries << " attempts. Server may remain out of sync." << std::endl;
                }
            }).detach();  // Detach the thread to let it run independently
        }
    }).detach();  // Detach the main recovery thread
}

} // namespace network
