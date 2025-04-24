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
    
    std::cout << "Received replication request for file: " << request->filename() << std::endl;
    
    // Perform the actual local loading 
    std::string filename = request->filename(); 
    std::string csv_data(request->csv_data()); 
    
    try { 
        ColumnStore column_store = csv::parse_csv(csv_data); 
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get()); 
        if (mem_state) { 
            mem_state->add_csv_file(filename, column_store.column_names, column_store.columns); 
            response->set_success(true); 
            response->set_message("Replication processed (stub implementation).");
            return Status::OK;
        } else { 
            response->set_success(false); 
            response->set_message("Internal error: State machine not available"); 
            return Status(grpc::StatusCode::INTERNAL, "State machine unavailable");
        } 
    } catch (const std::exception& e) { 
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
            std::cout << "Successfully deleted row " << row_index << " from file " << filename << std::endl;
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
            std::cout << "Successfully inserted row into file " << filename << std::endl;
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

// Stub implementations for unimplemented RPCs and methods
Status CsvServiceImpl::ComputeSum(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    std::cout << "ComputeSum called for file: " << filename << ", column: " << column_name << std::endl;

    try {
        // Read operations are typically safe to perform on local state (follower or leader)
        TableView view = state_->view(filename);
        if (view.empty()) {
            std::string msg = "File not found or empty: " + filename;
            response->set_success(false);
            response->set_message(msg);
            std::cerr << "ComputeSum failed: " << msg << std::endl;
            return Status(grpc::StatusCode::NOT_FOUND, msg);
        }

        double sum = view.compute_sum(column_name);
        response->set_value(sum);
        response->set_success(true);
        response->set_message("Sum computed successfully.");
        std::cout << "Successfully computed sum for " << filename << ":" << column_name << std::endl;
        return Status::OK;

    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_message(std::string("Error computing sum: ") + e.what());
        std::cerr << "ComputeSum exception for " << filename << ":" << column_name << ": " << e.what() << std::endl;
        // Determine appropriate status code (e.g., NOT_FOUND if column doesn't exist, INVALID_ARGUMENT if non-numeric)
        // For simplicity, using INTERNAL here, but could be refined.
        return Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

Status CsvServiceImpl::ComputeAverage(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    std::cout << "ComputeAverage called for file: " << filename << ", column: " << column_name << std::endl;

    try {
        // Read operations performed locally
        TableView view = state_->view(filename);
         if (view.empty()) {
            std::string msg = "File not found or empty: " + filename;
            response->set_success(false);
            response->set_message(msg);
            std::cerr << "ComputeAverage failed: " << msg << std::endl;
            return Status(grpc::StatusCode::NOT_FOUND, msg);
        }

        double average = view.compute_average(column_name);
        response->set_value(average);
        response->set_success(true);
        response->set_message("Average computed successfully.");
        std::cout << "Successfully computed average for " << filename << ":" << column_name << std::endl;
        return Status::OK;

    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_message(std::string("Error computing average: ") + e.what());
        std::cerr << "ComputeAverage exception for " << filename << ":" << column_name << ": " << e.what() << std::endl;
        return Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

Status CsvServiceImpl::ListLoadedFiles(ServerContext* context, const csvservice::Empty* request, csvservice::CsvFileList* response) {
    // Basic implementation - retrieve from state machine
    auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        std::vector<std::string> files = mem_state->list_files();
        for(const auto& file : files) {
            response->add_filenames(file);
        }
        return Status::OK;
    } else {
         return Status(grpc::StatusCode::INTERNAL, "State machine not available");
    }
}

Status CsvServiceImpl::GetClusterStatus(ServerContext* context, const csvservice::Empty* request, csvservice::ClusterStatusResponse* response) {
    // Implementation using ServerRegistry
    response->set_success(true);
    response->set_leader_address(registry_.get_leader());
    response->set_active_server_count(registry_.active_server_count());

    std::vector<std::string> all_servers = registry_.get_all_servers();
    for (const auto& server_addr : all_servers) {
        response->add_server_addresses(server_addr);
    }
    return Status::OK;
}

// Implementation for non-gRPC helper needed by menu
const std::unordered_map<std::string, ColumnStore>& CsvServiceImpl::get_loaded_files() const {
    auto* mem_state = dynamic_cast<const InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        return mem_state->get_files(); // Assuming InMemoryStateMachine has get_files() returning the map
    }
    // Return a static empty map if state machine is not available/correct type
    static const std::unordered_map<std::string, ColumnStore> empty_map;
    return empty_map;
}

} // namespace network
