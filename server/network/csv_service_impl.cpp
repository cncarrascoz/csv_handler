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
    return Status(grpc::StatusCode::UNIMPLEMENTED, "DeleteRow not implemented");
}

Status CsvServiceImpl::InsertRow(ServerContext* context, const csvservice::InsertRowRequest* request, csvservice::ModificationResponse* response) {
    return Status(grpc::StatusCode::UNIMPLEMENTED, "InsertRow not implemented");
}

// Stub implementations for unimplemented RPCs and methods
Status CsvServiceImpl::ComputeSum(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    return Status(grpc::StatusCode::UNIMPLEMENTED, "ComputeSum not implemented");
}

Status CsvServiceImpl::ComputeAverage(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    return Status(grpc::StatusCode::UNIMPLEMENTED, "ComputeAverage not implemented");
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
