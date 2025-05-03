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
#include "../../distributed/raft_node.hpp"
#include "../../storage/column_store.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using grpc::Channel;
using grpc::ClientContext;

namespace network {

CsvServiceImpl::CsvServiceImpl(ServerRegistry& registry)
    : registry_(registry), state_(std::make_unique<InMemoryStateMachine>()) // Simplified state machine for now
{ 
    // Initialize the Raft node with the state machine and server registry
    raft_node_ = std::make_shared<RaftNode>(
        registry_.get_self_address(),
        std::shared_ptr<IStateMachine>(state_.get(), [](IStateMachine*) {}), // Non-owning shared_ptr
        registry_.get_peer_addresses()
    );
    
    // Set up bidirectional relationship between Raft and ServerRegistry
    raft_node_->set_server_registry(&registry_);
    registry_.set_raft_node(raft_node_);
    
    // Start the Raft node
    raft_node_->start();
    
    std::cout << "CsvServiceImpl initialized with Raft node at " << registry_.get_self_address() << std::endl;
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
    int row_index = static_cast<int>(request->row_index());
    std::cout << "DeleteRow called for file: " << filename << ", row: " << row_index << std::endl;

    // Define the handler function to perform the actual deletion
    auto handler = [this, &filename, row_index, response](const csvservice::DeleteRowRequest*, csvservice::ModificationResponse* resp) -> grpc::Status {
        try {
            // Create a mutation for this delete operation
            Mutation mutation;
            mutation.file = filename;
            mutation.op = RowDelete{row_index};
            
            std::cout << "Created delete mutation for file " << filename 
                      << " at row " << row_index << std::endl;

            // If this node is the leader, submit the mutation to Raft
            if (registry_.is_leader() && raft_node_) {
                std::cout << "Leader submitting delete mutation to Raft for file: " << filename << std::endl;
                bool success = raft_node_->submit(mutation);
                
                if (success) {
                    resp->set_success(true);
                    resp->set_message("Row deleted successfully.");
                    std::cout << "Successfully submitted delete mutation to Raft for " << filename << std::endl;
                } else {
                    resp->set_success(false);
                    resp->set_message("Failed to submit delete mutation to Raft.");
                    std::cerr << "Failed to submit delete mutation to Raft for " << filename << std::endl;
                    return Status(grpc::StatusCode::INTERNAL, "Failed to submit to Raft");
                }
            } 
            // If we don't have a Raft node or are in standalone mode, apply directly
            else if (!raft_node_) {
                std::cout << "Applying delete mutation directly (no Raft) for file: " << filename << std::endl;
                state_->apply(mutation);
                
                resp->set_success(true);
                resp->set_message("Row deleted successfully.");
                std::cout << "Successfully deleted row from " << filename << " (direct application)" << std::endl;
            }
            // Otherwise, we shouldn't be here - the request should have been forwarded to the leader
            else {
                resp->set_success(false);
                resp->set_message("Not the leader, but request wasn't forwarded properly.");
                std::cerr << "DeleteRow error: Not leader but handling request for " << filename << std::endl;
                return Status(grpc::StatusCode::INTERNAL, "Not leader");
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
        // Response populated by forwarded call
        std::cout << "DeleteRow request forwarded to leader for file: " << filename << std::endl;
        return response->success() ? Status::OK : Status(grpc::StatusCode::INTERNAL, response->message());
    } else {
        // Executed locally
        return handler(request, response);
    }
}

Status CsvServiceImpl::InsertRow(ServerContext* context, const csvservice::InsertRowRequest* request, csvservice::ModificationResponse* response) {
    const std::string& filename = request->filename();
    std::cout << "InsertRow called for file: " << filename << std::endl;

    // Define the handler function to perform the actual insertion
    auto handler = [this, &filename, request, response](const csvservice::InsertRowRequest*, csvservice::ModificationResponse* resp) -> grpc::Status {
        try {
            // Create a mutation for this insert operation
            Mutation mutation;
            mutation.file = filename;
            
            // Create the row insert operation
            RowInsert insert_op;
            insert_op.values.reserve(request->values_size());
            for (const auto& val : request->values()) {
                insert_op.values.push_back(val);
            }
            mutation.op = insert_op;
            
            std::cout << "Created insert mutation for file " << filename 
                      << " with " << insert_op.values.size() << " values" << std::endl;

            // If this node is the leader, submit the mutation to Raft
            if (registry_.is_leader() && raft_node_) {
                std::cout << "Leader submitting insert mutation to Raft for file: " << filename << std::endl;
                bool success = raft_node_->submit(mutation);
                
                if (success) {
                    resp->set_success(true);
                    resp->set_message("Row inserted successfully.");
                    std::cout << "Successfully submitted insert mutation to Raft for " << filename << std::endl;
                } else {
                    resp->set_success(false);
                    resp->set_message("Failed to submit insert mutation to Raft.");
                    std::cerr << "Failed to submit insert mutation to Raft for " << filename << std::endl;
                    return Status(grpc::StatusCode::INTERNAL, "Failed to submit to Raft");
                }
            } 
            // If we don't have a Raft node or are in standalone mode, apply directly
            else if (!raft_node_) {
                std::cout << "Applying insert mutation directly (no Raft) for file: " << filename << std::endl;
                state_->apply(mutation);
                
                resp->set_success(true);
                resp->set_message("Row inserted successfully.");
                std::cout << "Successfully inserted row into " << filename << " (direct application)" << std::endl;
            }
            // Otherwise, we shouldn't be here - the request should have been forwarded to the leader
            else {
                resp->set_success(false);
                resp->set_message("Not the leader, but request wasn't forwarded properly.");
                std::cerr << "InsertRow error: Not leader but handling request for " << filename << std::endl;
                return Status(grpc::StatusCode::INTERNAL, "Not leader");
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

// Implementation of AppendEntries RPC
Status CsvServiceImpl::AppendEntries(ServerContext* context, 
                                    const csvservice::AppendEntriesRequest* request,
                                    csvservice::AppendEntriesResponse* response) {
    std::cout << "AppendEntries RPC received from: " << request->leader_id() << std::endl;
    
    // Check if Raft node is initialized
    if (!raft_node_) {
        std::cerr << "Error: Raft node not initialized" << std::endl;
        return Status(StatusCode::INTERNAL, "Raft node not initialized");
    }
    
    // Convert AppendEntriesRequest to our internal format
    RaftNode::AppendEntriesArgs args;
    args.term = request->term();
    args.leader_id = request->leader_id();
    args.prev_log_index = request->prev_log_index();
    args.prev_log_term = request->prev_log_term();
    args.leader_commit = request->leader_commit();
    
    // Convert entries
    for (const auto& entry : request->entries()) {
        RaftNode::LogEntry log_entry;
        log_entry.term = entry.term();
        
        // Convert mutation
        const auto& mutation = entry.mutation();
        log_entry.cmd.file = mutation.filename();
        
        if (mutation.has_row_insert()) {
            std::vector<std::string> values;
            for (const auto& value : mutation.row_insert().values()) {
                values.push_back(value);
            }
            log_entry.cmd.op = RowInsert{values};
        } else if (mutation.has_row_delete()) {
            log_entry.cmd.op = RowDelete{static_cast<int>(mutation.row_delete().row_index())};
        }
        
        args.entries.push_back(log_entry);
    }
    
    // Process the AppendEntries request
    RaftNode::AppendEntriesReply reply;
    raft_node_->handle_append_entries_safe(args, reply);
    
    // Convert reply to gRPC response
    response->set_term(reply.term);
    response->set_success(reply.success);
    response->set_conflict_index(reply.conflict_index);
    response->set_conflict_term(reply.conflict_term);
    
    return Status::OK;
}

Status CsvServiceImpl::RequestVote(ServerContext* context,
                                  const csvservice::RequestVoteRequest* request,
                                  csvservice::RequestVoteResponse* response) {
    std::cout << "RequestVote RPC received from: " << request->candidate_id() << std::endl;
    
    // Check if Raft node is initialized
    if (!raft_node_) {
        std::cerr << "Error: Raft node not initialized" << std::endl;
        return Status(StatusCode::INTERNAL, "Raft node not initialized");
    }
    
    // Convert RequestVoteRequest to our internal format
    RaftNode::RequestVoteArgs args;
    args.term = request->term();
    args.candidate_id = request->candidate_id();
    args.last_log_index = request->last_log_index();
    args.last_log_term = request->last_log_term();
    
    // Process the RequestVote request
    RaftNode::RequestVoteReply reply;
    raft_node_->handle_request_vote_safe(args, reply);
    
    // Convert reply to gRPC response
    response->set_term(reply.term);
    response->set_vote_granted(reply.vote_granted);
    
    return Status::OK;
}

// Implementation of Heartbeat RPC
grpc::Status CsvServiceImpl::Heartbeat(grpc::ServerContext* context, 
                                      const csvservice::HeartbeatRequest* request, 
                                      csvservice::HeartbeatResponse* response) {
    std::cout << "Heartbeat received from: " << request->server_address() << std::endl;
    
    // Update leader information in response
    response->set_success(true);
    response->set_leader_address(registry_.get_leader());
    
    return grpc::Status::OK;
}

// Stub for ApplyMutation (called by peers when leader replicates)
grpc::Status CsvServiceImpl::ApplyMutation(grpc::ServerContext* context,
                                          const csvservice::ReplicateMutationRequest* request,
                                          csvservice::ReplicateMutationResponse* response) {
    // Check if this is a Raft AppendEntries proxy request
    if (request->filename() == "raft_heartbeat" || 
        request->filename() == "raft_append_entries_header" ||
        (request->has_row_insert() && !request->row_insert().values().empty() && 
         request->row_insert().values(0).find("|") != std::string::npos)) {
        
        // This is a Raft AppendEntries proxy request
        // Forward it to the RaftNode
        if (raft_node_) {
            raft_node_->handle_append_entries_rpc(*request, *response);
        } else {
            response->set_success(false);
        }
        return grpc::Status::OK;
    }

    // Regular mutation handling
    std::cout << "Received mutation for file: " << request->filename() << std::endl;
    
    // If we're not the leader and this is a client request, redirect to the leader
    if (raft_node_ && raft_node_->role() != ServerRole::LEADER && raft_node_->role() != ServerRole::STANDALONE) {
        std::string leader = raft_node_->current_leader();
        if (!leader.empty()) {
            response->set_success(false);
            response->set_message("Not the leader. Current leader: " + leader);
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Not the leader");
        }
    }
    
    // Create a Mutation object from the request
    Mutation mutation;
    mutation.file = request->filename();
    
    if (request->has_row_insert()) {
        const auto& insert = request->row_insert();
        std::vector<std::string> values;
        for (const auto& value : insert.values()) {
            values.push_back(value);
        }
        mutation.op = RowInsert{values};
    } else if (request->has_row_delete()) {
        const auto& del = request->row_delete();
        mutation.op = RowDelete{static_cast<int>(del.row_index())};
    } else {
        response->set_success(false);
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid mutation type");
    }
    
    // Apply the mutation using Raft
    bool success = false;
    if (raft_node_) {
        // Submit the mutation to Raft
        success = raft_node_->submit(mutation);
    } else {
        // Apply directly to the state machine (legacy mode)
        try {
            state_->apply(mutation);
            success = true;
        } catch (const std::exception& e) {
            response->set_success(false);
            return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
        }
    }
    
    response->set_success(success);
    return grpc::Status::OK;
}

// Stub for RegisterPeer
grpc::Status CsvServiceImpl::RegisterPeer(grpc::ServerContext* context,
                                         const csvservice::RegisterPeerRequest* request,
                                         csvservice::RegisterPeerResponse* response) {
    std::string peer_address = request->peer_address();
    
    // Check if this is a Raft RequestVote proxy
    if (peer_address.find('|') != std::string::npos) {
        // This is a Raft RequestVote proxy request
        // Format: candidate_id|term|last_log_index|last_log_term
        response->set_success(true);
        response->set_leader_address(registry_.get_leader());
        return grpc::Status::OK;
    }
    
    // Regular peer registration
    registry_.register_peer(peer_address);
    
    response->set_success(true);
    response->set_message("Peer registered successfully");
    
    // Always include the leader address in the response
    response->set_leader_address(registry_.get_leader());
    
    return grpc::Status::OK;
}

// Helper for replicate_mutation_async (called by leader after local insert/delete)
void CsvServiceImpl::replicate_mutation_async(const Mutation& mutation) {
    // If we have a Raft node, use it for replication
    if (raft_node_) {
        // Submit the mutation to Raft
        bool success = raft_node_->submit(mutation);
        if (!success) {
            std::cerr << "Failed to submit mutation to Raft: " << mutation.file << std::endl;
        }
        return;
    }
    
    // Legacy replication logic (without Raft)
    std::cout << "Asynchronously replicating " << mutation.file << " to peers..." << std::endl;
    
    // Get all peers from registry
    auto peers = registry_.get_peer_addresses();
    
    // Create a ReplicateMutationRequest from the Mutation
    csvservice::ReplicateMutationRequest request;
    request.set_filename(mutation.file);
    
    if (mutation.has_insert()) {
        auto* insert = request.mutable_row_insert();
        for (const auto& value : mutation.insert().values) {
            insert->add_values(value);
        }
    } else if (mutation.has_delete()) {
        auto* del = request.mutable_row_delete();
        del->set_row_index(mutation.del().row_index);
    }
    
    // For each peer, send the mutation asynchronously
    for (const auto& peer_address : peers) {
        std::thread([this, peer_address, request]() {
            // Create a new channel and stub for this peer
            auto channel = grpc::CreateChannel(peer_address, grpc::InsecureChannelCredentials());
            auto stub = csvservice::CsvService::NewStub(channel);
            
            // Set up RPC
            grpc::ClientContext context;
            csvservice::ReplicateMutationResponse response;
            
            // Set a timeout
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
            
            // Send the RPC
            auto status = stub->ApplyMutation(&context, request, &response);
            
            if (!status.ok()) {
                std::cerr << "Failed to replicate mutation to " << peer_address 
                          << ": " << status.error_message() << std::endl;
            } else if (!response.success()) {
                std::cerr << "Peer " << peer_address << " rejected mutation: " 
                          << response.message() << std::endl;
            }
        }).detach();
    }
}

// Implementation of ComputeSum
Status CsvServiceImpl::ComputeSum(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    std::cout << "ComputeSum called for file: " << filename 
              << ", column: " << column_name << std::endl;
    
    try {
        // Get the file from the state machine
        TableView view = state_->view(filename);
        
        // Use the compute_sum method of the TableView class
        double sum = view.compute_sum(column_name);
        
        response->set_success(true);
        response->set_message("Sum calculated successfully.");
        response->set_value(sum);
        
        std::cout << "Sum of column '" << column_name << "' in file '" 
                  << filename << "': " << sum << std::endl;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_message(std::string("Error computing sum: ") + e.what());
        response->set_value(0.0);
        
        std::cerr << "ComputeSum exception for " << filename << ": " 
                  << e.what() << std::endl;
    }
    
    return Status::OK;
}

// Implementation of ComputeAverage
Status CsvServiceImpl::ComputeAverage(ServerContext* context, const csvservice::ColumnOperationRequest* request, csvservice::NumericResponse* response) {
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    std::cout << "ComputeAverage called for file: " << filename 
              << ", column: " << column_name << std::endl;
    
    try {
        // Get the file from the state machine
        TableView view = state_->view(filename);
        
        // Use the compute_average method of the TableView class
        double avg = view.compute_average(column_name);
        
        response->set_success(true);
        response->set_message("Average calculated successfully.");
        response->set_value(avg);
        
        std::cout << "Average of column '" << column_name << "' in file '" 
                  << filename << "': " << avg << std::endl;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_message(std::string("Error computing average: ") + e.what());
        response->set_value(0.0);
        
        std::cerr << "ComputeAverage exception for " << filename << ": " 
                  << e.what() << std::endl;
    }
    
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
     response->set_message("Cluster status fetched (stub)"); // Added semicolon
     return Status::OK;
}

// Stub for get_loaded_files (Internal helper used by menu.cpp)
// Match return type from header: const std::unordered_map<std::string, ColumnStore>&
const std::unordered_map<std::string, ColumnStore>& CsvServiceImpl::get_loaded_files() const {
    std::cout << "[Stub] get_loaded_files called." << std::endl;
    // TODO: Implement actual logic to get files from state_
    auto* mem_state = dynamic_cast<const InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        // Assuming InMemoryStateMachine has a method returning the map ref:
        // return mem_state->get_all_files(); 
        // For the stub, return a static empty map to match the signature
        static const std::unordered_map<std::string, ColumnStore> empty_map;
        return empty_map;
    }
    // Return static empty map if state machine not available
    static const std::unordered_map<std::string, ColumnStore> empty_map_fallback;
    return empty_map_fallback;
}


} // namespace network
