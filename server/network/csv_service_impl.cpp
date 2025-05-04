#include "csv_service_impl.hpp"
#include "storage/csv_parser.hpp"
#include "core/TableView.hpp"
#include "core/Mutation.hpp"
#include "server_registry.hpp"
#include <grpcpp/grpcpp.h>
#include "storage/InMemoryStateMachine.hpp"
#include "persistence/PersistenceManager.hpp"
#include <memory>
#include <chrono>
#include <thread>
#include <future>
#include <functional>
#include <iostream>
#include <numeric>
#include <limits>
#include <filesystem>

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using grpc::Channel;
using grpc::ClientContext;

namespace network {

CsvServiceImpl::CsvServiceImpl(ServerRegistry& registry)
    : registry_(registry), 
      state_(std::make_shared<InMemoryStateMachine>())
{ 
    // Initialize the service
    initialize();
    
    // Register for leader change notifications
    registry_.set_leader_change_callback([this](const std::string& new_leader) {
        this->handle_leader_change(new_leader);
    });
} 

CsvServiceImpl::~CsvServiceImpl() {
    std::cout << "CsvServiceImpl destructor called" << std::endl;
    
    // Ensure any pending persistence operations are completed
    if (persistence_manager_) {
        // Create a final snapshot before shutting down
        persistence_manager_->create_snapshot();
    }
}

// Initialize the service
void CsvServiceImpl::initialize() {
    std::cout << "Initializing CsvServiceImpl..." << std::endl;
    
    // Create data and log directories if they don't exist
    std::filesystem::create_directories("data");
    std::filesystem::create_directories("logs");
    
    // Initialize the persistence manager
    persistence_manager_ = std::make_unique<PersistenceManager>(
        state_,
        "data", // Data directory for snapshots
        "logs"  // Log directory for WAL
    );
    
    // Recover state from disk
    bool recovery_success = persistence_manager_->initialize();
    
    if (recovery_success) {
        std::cout << "Successfully recovered state from disk" << std::endl;
    } else {
        std::cout << "No state recovered from disk or recovery failed" << std::endl;
    }
    
    // Update the loaded files cache
    update_loaded_files_cache();
    
    std::cout << "CsvServiceImpl initialization complete" << std::endl;
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
        
        // Add the file to the in-memory state machine
        state_->add_csv_file(filename, column_store.column_names, column_store.columns);
        
        // Persist the data using the PersistenceManager
        if (persistence_manager_) {
            std::cout << "Persisting uploaded CSV data to disk..." << std::endl;
            
            // For each row, create and observe a mutation
            if (!column_store.column_names.empty() && !column_store.columns.empty()) {
                size_t num_rows = column_store.columns[column_store.column_names[0]].size();
                
                for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
                    Mutation mutation;
                    mutation.file = filename;
                    
                    RowInsert insert;
                    for (const auto& col_name : column_store.column_names) {
                        insert.values.push_back(column_store.columns[col_name][row_idx]);
                    }
                    
                    mutation.op = insert;
                    persistence_manager_->observe_mutation(mutation);
                }
                
                // Create a snapshot after uploading a file
                persistence_manager_->create_snapshot();
                std::cout << "CSV data persisted to disk successfully" << std::endl;
            }
        }
        
        // Update the loaded files cache
        update_loaded_files_cache();
         
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
        return Status::OK; // Return OK status but with error in response 
    } 
     
    // 2. Replicate to Peers if we're the leader 
    if (registry_.is_leader()) { 
        std::vector<std::string> peers = registry_.get_peer_addresses(); 
        if (!peers.empty()) { 
            std::cout << "Replicating upload to " << peers.size() << " peers..." << std::endl; 
             
            // Create a thread pool or use async operations for parallel replication 
            std::vector<std::future<bool>> replication_futures; 
             
            for (const auto& peer_address : peers) { 
                // Use async to replicate in parallel 
                auto future = std::async(std::launch::async, [peer_address, request]() -> bool { 
                    std::cout << "Replicating to peer: " << peer_address << std::endl; 
                     
                    // Create channel and stub for the peer 
                    auto channel = grpc::CreateChannel(peer_address, grpc::InsecureChannelCredentials()); 
                    auto stub = csvservice::CsvService::NewStub(channel); 
                     
                    // Set up context with timeout 
                    grpc::ClientContext context; 
                    std::chrono::system_clock::time_point deadline = 
                        std::chrono::system_clock::now() + std::chrono::seconds(10); 
                    context.set_deadline(deadline); 
                     
                    // Call ReplicateUpload RPC 
                    csvservice::ReplicateUploadResponse peer_response; 
                    auto status = stub->ReplicateUpload(&context, *request, &peer_response); 
                     
                    if (!status.ok()) { 
                        std::cerr << "Failed to replicate to " << peer_address << ": " 
                                  << status.error_message() << std::endl; 
                        return false; 
                    } 
                     
                    if (!peer_response.success()) { 
                        std::cerr << "Peer " << peer_address << " reported error: " 
                                  << peer_response.message() << std::endl; 
                        return false; 
                    } 
                     
                    std::cout << "Successfully replicated to " << peer_address << std::endl; 
                    return true; 
                }); 
                 
                replication_futures.push_back(std::move(future)); 
            } 
             
            // Wait for all replications to complete 
            int success_count = 0; 
            for (auto& future : replication_futures) { 
                if (future.get()) { 
                    success_count++; 
                } 
            } 
             
            std::cout << "Replication completed: " << success_count << "/" << peers.size() 
                      << " peers successful" << std::endl; 
             
            // If we couldn't replicate to all peers, we might want to log this 
            // but still return success to the client if at least one peer got it 
            if (success_count < peers.size()) { 
                std::cerr << "Warning: Could not replicate to all peers" << std::endl; 
                // We could add this info to the response message if desired 
            } 
        } 
    } 
     
    return Status::OK; 
}

Status CsvServiceImpl::ViewFile(ServerContext* context, 
                                const csvservice::ViewFileRequest* request, 
                                csvservice::ViewFileResponse* response) {
    std::string filename = request->filename();
    std::cout << "ViewFile request for: " << filename << std::endl;
    
    try {
        // Get the file data from the state machine
        TableView view = state_->view(filename);
        
        // Set the column names in the response
        for (const auto& col_name : view.column_names) {
            response->add_column_names(col_name);
        }
        
        // Set the row data in the response
        for (int row = 0; row < view.row_count(); ++row) {
            auto* row_data = response->add_rows();
            for (const auto& col_name : view.column_names) {
                // Access the column data directly
                row_data->add_values(view.columns.at(col_name).at(row));
            }
        }
        
        response->set_success(true);
        response->set_message("File retrieved successfully");
    } catch (const std::exception& e) {
        std::cerr << "Error viewing file " << filename << ": " << e.what() << std::endl;
        response->set_success(false);
        response->set_message(std::string("Failed to view file: ") + e.what());
    }
    
    return Status::OK;
}

Status CsvServiceImpl::DeleteRow(ServerContext* context, const csvservice::DeleteRowRequest* request, csvservice::ModificationResponse* response) {
    // Check if we need to forward to the leader
    auto handler = [this](const csvservice::DeleteRowRequest* req, csvservice::ModificationResponse* res) -> Status {
        return this->DeleteRow(nullptr, req, res);
    };
    
    if (forward_to_leader_if_needed(request, response, handler)) {
        return Status::OK;
    }
    
    // We are the leader, process the request
    std::string filename = request->filename();
    int row_index = request->row_index();
    
    try {
        // Create a delete mutation
        Mutation mutation;
        mutation.file = filename;
        RowDelete del;
        del.row_index = row_index;
        mutation.op = del;
        
        // Apply the mutation to the in-memory state machine
        state_->apply(mutation);
        
        // Persist the mutation
        if (persistence_manager_) {
            persistence_manager_->observe_mutation(mutation);
        }
        
        // Update the loaded files cache
        update_loaded_files_cache();
        
        // Replicate the mutation to peers
        replicate_mutation_async(mutation);
        
        response->set_success(true);
        response->set_message("Row deleted successfully");
    } catch (const std::exception& e) {
        std::cerr << "Error deleting row " << row_index << " from " << filename << ": " << e.what() << std::endl;
        response->set_success(false);
        response->set_message(std::string("Failed to delete row: ") + e.what());
    }
    
    return Status::OK;
}

Status CsvServiceImpl::InsertRow(ServerContext* context, const csvservice::InsertRowRequest* request, csvservice::ModificationResponse* response) {
    // Check if we need to forward to the leader
    auto handler = [this](const csvservice::InsertRowRequest* req, csvservice::ModificationResponse* res) -> Status {
        return this->InsertRow(nullptr, req, res);
    };
    
    if (forward_to_leader_if_needed(request, response, handler)) {
        return Status::OK;
    }
    
    // We are the leader, process the request
    std::string filename = request->filename();
    std::vector<std::string> values;
    for (const auto& value : request->values()) {
        values.push_back(value);
    }
    
    try {
        // Create an insert mutation
        Mutation mutation;
        mutation.file = filename;
        RowInsert insert;
        insert.values = values;
        mutation.op = insert;
        
        // Apply the mutation to the in-memory state machine
        state_->apply(mutation);
        
        // Persist the mutation
        if (persistence_manager_) {
            persistence_manager_->observe_mutation(mutation);
        }
        
        // Update the loaded files cache
        update_loaded_files_cache();
        
        // Replicate the mutation to peers
        replicate_mutation_async(mutation);
        
        response->set_success(true);
        response->set_message("Row inserted successfully");
    } catch (const std::exception& e) {
        std::cerr << "Error inserting row into " << filename << ": " << e.what() << std::endl;
        response->set_success(false);
        response->set_message(std::string("Failed to insert row: ") + e.what());
    }
    
    return Status::OK;
}

// Internal RPC used by the leader to replicate an upload to peers
Status CsvServiceImpl::ReplicateUpload(
    ServerContext* context,
    const csvservice::CsvUploadRequest* request,
    csvservice::ReplicateUploadResponse* response) {
    
    std::string filename = request->filename();
    std::string csv_data = request->csv_data();
    
    try {
        // Parse the CSV data
        ColumnStore column_store = csv::parse_csv(csv_data);
        
        // Add to in-memory state machine
        state_->add_csv_file(filename, column_store.column_names, column_store.columns);
        
        // Persist the data
        if (persistence_manager_) {
            // For each row, create and observe a mutation
            for (size_t row_idx = 0; row_idx < column_store.columns[column_store.column_names[0]].size(); ++row_idx) {
                Mutation mutation;
                mutation.file = filename;
                
                RowInsert insert;
                for (const auto& col_name : column_store.column_names) {
                    insert.values.push_back(column_store.columns[col_name][row_idx]);
                }
                
                mutation.op = insert;
                persistence_manager_->observe_mutation(mutation);
            }
        }
        
        // Update the loaded files cache
        update_loaded_files_cache();
        
        // Set success response
        response->set_success(true);
        response->set_message("Successfully replicated upload");
        std::cout << "Successfully replicated upload for " << filename << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error replicating upload for " << filename << ": " << e.what() << std::endl;
        response->set_success(false);
        response->set_message(std::string("Failed to replicate upload: ") + e.what());
    }
    
    return Status::OK;
}

// --- STUB IMPLEMENTATIONS for methods causing linker errors ---

// Stub for ApplyMutation (called by peers when leader replicates)
Status CsvServiceImpl::ApplyMutation(ServerContext* context, const csvservice::ReplicateMutationRequest* request, csvservice::ReplicateMutationResponse* response) {
    std::string filename = request->filename();
    
    try {
        // Create a mutation from the request
        Mutation mutation;
        mutation.file = filename;
        
        if (request->has_row_insert()) {
            RowInsert insert;
            for (const auto& value : request->row_insert().values()) {
                insert.values.push_back(value);
            }
            mutation.op = insert;
        } else if (request->has_row_delete()) {
            RowDelete del;
            del.row_index = request->row_delete().row_index();
            mutation.op = del;
        } else {
            throw std::runtime_error("Unknown mutation type");
        }
        
        // Apply the mutation to the in-memory state machine
        state_->apply(mutation);
        
        // Persist the mutation
        if (persistence_manager_) {
            persistence_manager_->observe_mutation(mutation);
        }
        
        // Update the loaded files cache
        update_loaded_files_cache();
        
        response->set_success(true);
        response->set_message("Mutation applied successfully");
    } catch (const std::exception& e) {
        std::cerr << "Error applying mutation to " << filename << ": " << e.what() << std::endl;
        response->set_success(false);
        response->set_message(std::string("Failed to apply mutation: ") + e.what());
    }
    
    return Status::OK;
}

// Stub for replicate_mutation_async (called by leader after local insert/delete)
void CsvServiceImpl::replicate_mutation_async(const Mutation& mutation) {
    // Get all peer addresses
    std::vector<std::string> peers = registry_.get_peer_addresses();
    if (peers.empty()) {
        return;
    }
    
    std::cout << "Replicating mutation to " << peers.size() << " peers..." << std::endl;
    
    // Create a thread pool or use async operations for parallel replication
    std::vector<std::future<bool>> replication_futures;
    
    for (const auto& peer_address : peers) {
        // Use async to replicate in parallel
        auto future = std::async(std::launch::async, [this, peer_address, &mutation]() -> bool {
            std::cout << "Replicating mutation to peer: " << peer_address << std::endl;
            
            // Create channel and stub for the peer
            auto channel = grpc::CreateChannel(peer_address, grpc::InsecureChannelCredentials());
            auto stub = csvservice::CsvService::NewStub(channel);
            
            // Set up context with timeout
            grpc::ClientContext context;
            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() + std::chrono::seconds(5);
            context.set_deadline(deadline);
            
            // Create the request
            csvservice::ReplicateMutationRequest request;
            request.set_filename(mutation.file);
            
            if (std::holds_alternative<RowInsert>(mutation.op)) {
                auto insert = std::get<RowInsert>(mutation.op);
                auto* row_insert = request.mutable_row_insert();
                for (const auto& value : insert.values) {
                    row_insert->add_values(value);
                }
            } else if (std::holds_alternative<RowDelete>(mutation.op)) {
                auto del = std::get<RowDelete>(mutation.op);
                auto* row_delete = request.mutable_row_delete();
                row_delete->set_row_index(del.row_index);
            }
            
            // Call ApplyMutation RPC
            csvservice::ReplicateMutationResponse response;
            auto status = stub->ApplyMutation(&context, request, &response);
            
            if (!status.ok()) {
                std::cerr << "Failed to replicate mutation to " << peer_address << ": "
                          << status.error_message() << std::endl;
                return false;
            }
            
            if (!response.success()) {
                std::cerr << "Peer " << peer_address << " reported error: "
                          << response.message() << std::endl;
                return false;
            }
            
            std::cout << "Successfully replicated mutation to " << peer_address << std::endl;
            return true;
        });
        
        replication_futures.push_back(std::move(future));
    }
    
    // Wait for all replications to complete
    int success_count = 0;
    for (auto& future : replication_futures) {
        if (future.get()) {
            success_count++;
        }
    }
    
    std::cout << "Mutation replication completed: " << success_count << "/" << peers.size()
              << " peers successful" << std::endl;
}

// Stub for ComputeSum
Status CsvServiceImpl::ComputeSum(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    std::cout << "[Stub] ComputeSum called." << std::endl;
    // TODO: Implement sum logic using state_->view()
    response->set_success(true);
    response->set_message("Sum calculation stub.");
    response->set_value(0.0); // Default value
    return Status::OK;
}

// Stub for ComputeAverage
Status CsvServiceImpl::ComputeAverage(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    std::cout << "[Stub] ComputeAverage called." << std::endl;
    // TODO: Implement average logic using state_->view()
    response->set_success(true);
    response->set_message("Average calculation stub.");
    response->set_value(0.0); // Default value
    return Status::OK;
}

// Stub for ListLoadedFiles (RPC)
Status CsvServiceImpl::ListLoadedFiles(ServerContext* context, const csvservice::Empty* request, csvservice::CsvFileList* response) {
    std::cout << "[Stub] ListLoadedFiles RPC called." << std::endl;
    
    // Get file list from state machine
    const auto& files = state_->list_files();
    for (const auto& filename : files) {
        response->add_filenames(filename);
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
     response->set_message("Cluster status fetched (stub)");
     return Status::OK;
}

// Helper method to update the loaded files cache
void CsvServiceImpl::update_loaded_files_cache() {
    std::lock_guard<std::shared_mutex> lock(files_mutex_);
    
    // Clear the cache
    loaded_files_cache_.clear();
    
    // Get the list of files from the state machine
    const auto& files = state_->list_files();
    
    // For each file, get the column store and add it to the cache
    for (const auto& filename : files) {
        TableView view = state_->view(filename);
        
        ColumnStore column_store;
        column_store.column_names = view.column_names;
        column_store.columns = view.columns;
        
        loaded_files_cache_[filename] = column_store;
    }
}

// Helper method to handle leader change events
void CsvServiceImpl::handle_leader_change(const std::string& new_leader) {
    std::cout << "CsvServiceImpl notified of leader change to: " << new_leader << std::endl;

    // If this server is now the leader, initialize leader state
    if (new_leader == registry_.get_self_address()) {
        std::cout << "THIS SERVER IS NOW THE LEADER - Ready to handle client requests directly" << std::endl;

        // Ensure state machine is ready for leader operations
        if (state_) {
            std::cout << "Initializing leader state machine and replication pipeline" << std::endl;
            // Refresh internal state if needed
            update_loaded_files_cache();
        } else {
            std::cerr << "ERROR: State machine is null during leader transition!" << std::endl;
        }

        // Log readiness to accept client requests
        std::cout << "Leader initialization complete - Now accepting client requests and handling replication" << std::endl;
    } else {
        std::cout << "Leader changed to: " << new_leader << " - Will forward client requests to new leader" << std::endl;
    }
}

} // namespace network
