#include "raft_node.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "../proto/csv_service.grpc.pb.h"

#include "../core/Mutation.hpp"
#include "../server/network/server_registry.hpp"

/**
 * Constructor - initializes the Raft node with the given parameters
 * 
 * @param node_id Unique identifier for this node
 * @param state_machine State machine to apply operations to
 * @param peer_addresses Addresses of other nodes in the cluster
 */
RaftNode::RaftNode(
    const std::string& node_id,
    std::shared_ptr<IStateMachine> state_machine,
    const std::vector<std::string>& peer_addresses)
    : node_id_(node_id),
      state_machine_(state_machine),
      peer_addresses_(peer_addresses),
      // Initialize log with a dummy entry at index 0 (Raft logs are 1-indexed)
      log_({{0, {}}}),
      // Initialize random number generator with a random seed
      rng_(std::random_device{}()),
      // Start in STANDALONE mode if no peers, otherwise FOLLOWER
      role_(peer_addresses.empty() ? ServerRole::STANDALONE : ServerRole::FOLLOWER),
      // Initialize Raft state
      current_term_(0),
      commit_index_(0),
      last_applied_(0),
      running_(false) {
    
    // Initialize leader state (will be properly set when becoming leader)
    next_index_.resize(peer_addresses_.size(), 1);
    match_index_.resize(peer_addresses_.size(), 0);
    
    // Set initial election and heartbeat deadlines
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(150);
    heartbeat_deadline_ = std::chrono::steady_clock::now();
    
    // Create gRPC stubs for peer communication
    create_peer_stubs();
    
    std::cout << "RaftNode initialized: " << node_id_ 
              << " with " << peer_addresses_.size() << " peers" << std::endl;
}

/**
 * Destructor - ensures all threads are stopped
 */
RaftNode::~RaftNode() {
    stop();
}

/**
 * Creates gRPC stubs for communicating with peer nodes
 */
void RaftNode::create_peer_stubs() {
    for (const auto& peer_address : peer_addresses_) {
        // Create a channel to the peer
        auto channel = grpc::CreateChannel(
            peer_address, grpc::InsecureChannelCredentials());
        
        // Create a stub using the channel
        auto stub = csvservice::CsvService::NewStub(channel);
        
        // Store the stub
        peer_stubs_.push_back(std::move(stub));
    }
}

/**
 * Start the Raft node - begins the ticker thread and apply thread
 */
void RaftNode::start() {
    // If already running, do nothing
    if (running_.exchange(true)) {
        return;
    }
    
    std::cout << "Starting RaftNode: " << node_id_ << " in " 
              << (role_ == ServerRole::STANDALONE ? "STANDALONE" : "FOLLOWER") 
              << " mode" << std::endl;
    
    // Only start threads if we have peers (otherwise we're in standalone mode)
    if (!peer_addresses_.empty()) {
        // Start the ticker thread for heartbeats, elections, etc.
        ticker_thread_ = std::thread(&RaftNode::tick, this);
        
        // Start the apply thread for applying committed entries
        apply_thread_ = std::thread(&RaftNode::apply_entries_loop, this);
    }
}

/**
 * Stop the Raft node - stops all threads
 */
void RaftNode::stop() {
    // If already stopped, do nothing
    if (!running_.exchange(false)) {
        return;
    }
    
    // Notify apply thread to wake up and exit
    apply_cv_.notify_all();
    
    // Wait for threads to finish
    if (ticker_thread_.joinable()) {
        ticker_thread_.join();
    }
    
    if (apply_thread_.joinable()) {
        apply_thread_.join();
    }
    
    std::cout << "Stopped RaftNode: " << node_id_ << std::endl;
}

/**
 * Submit a mutation to the Raft consensus algorithm
 * 
 * @param mutation The mutation to submit
 * @return True if the mutation was successfully submitted
 */
bool RaftNode::submit(const Mutation& mutation) {
    // Lock to safely access shared state
    std::lock_guard<std::mutex> lock(mu_);
    
    // Only the leader can submit mutations
    if (role_ != ServerRole::LEADER && role_ != ServerRole::STANDALONE) {
        std::cout << "Cannot submit mutation: not the leader" << std::endl;
        return false;
    }
    
    // In standalone mode, apply directly to the state machine
    if (role_ == ServerRole::STANDALONE) {
        state_machine_->apply(mutation);
        return true;
    }
    
    // Create a new log entry
    LogEntry entry;
    entry.term = current_term_;
    entry.cmd = mutation;
    
    // Append the entry to the log
    log_.push_back(entry);
    
    std::cout << "Leader " << node_id_ << " appended mutation to log (index=" 
              << log_.size() - 1 << ")" << std::endl;
    
    // Update leader's matchIndex for itself
    match_index_[match_index_.size() - 1] = log_.size() - 1;
    
    // Try to replicate the new entry to followers immediately
    send_append_entries_to_all(false);
    
    return true;
}

/**
 * Get the current role of this node
 */
ServerRole RaftNode::role() const {
    return role_;
}

/**
 * Get the current term
 */
uint64_t RaftNode::current_term() const {
    return current_term_;
}

/**
 * Get the ID of the current leader
 */
std::string RaftNode::current_leader() const {
    return current_leader_;
}

/**
 * Main ticker function - handles periodic tasks based on node role
 * This runs in a separate thread and is responsible for:
 * - Triggering elections when election timeout expires
 * - Sending heartbeats when heartbeat timeout expires
 */
void RaftNode::tick() {
    std::cout << "Ticker thread started for node: " << node_id_ << std::endl;
    
    // Continue running until the node is stopped
    while (running_) {
        // Sleep for a short time to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        // Get the current time
        auto now = std::chrono::steady_clock::now();
        
        // Lock to safely access shared state
        std::lock_guard<std::mutex> lock(mu_);
        
        // Handle different roles
        switch (role_) {
            case ServerRole::FOLLOWER:
            case ServerRole::CANDIDATE:
                // Check if election timeout has expired
                if (now >= election_deadline_) {
                    std::cout << "Election timeout expired for " << node_id_ << std::endl;
                    // Start an election
                    become_candidate();
                    // Reset the election timeout
                    reset_election_deadline();
                }
                break;
                
            case ServerRole::LEADER:
                // Check if heartbeat timeout has expired
                if (now >= heartbeat_deadline_) {
                    // Send heartbeats to all followers
                    send_append_entries_to_all(true);
                    // Reset the heartbeat timeout (typically 50-100ms)
                    heartbeat_deadline_ = now + std::chrono::milliseconds(50);
                }
                break;
                
            case ServerRole::STANDALONE:
                // Nothing to do in standalone mode
                break;
        }
    }
    
    std::cout << "Ticker thread stopped for node: " << node_id_ << std::endl;
}

/**
 * Thread function for applying committed entries to the state machine
 */
void RaftNode::apply_entries_loop() {
    std::cout << "Node " << node_id_ << " started apply thread" << std::endl;
    
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            
            // Wait until there are entries to apply or the node is shutting down
            apply_cv_.wait(lock, [this] {
                return !running_ || commit_index_ > last_applied_;
            });
            
            // If we're shutting down, exit the thread
            if (!running_) {
                break;
            }
            
            // Apply committed entries to the state machine
            apply_entries();
        }
        
        // Sleep a bit to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    std::cout << "Node " << node_id_ << " stopped apply thread" << std::endl;
}

/**
 * Step down to follower when a higher term is seen
 * 
 * @param term The new term to adopt
 */
void RaftNode::step_down(uint64_t term) {
    if (term > current_term_) {
        std::cout << "Node " << node_id_ << " stepping down: term " 
                  << current_term_ << " -> " << term << std::endl;
        
        // Update term and clear vote
        current_term_ = term;
        voted_for_ = "";
        
        // Become a follower
        become_follower(term, "");
    }
}

/**
 * Transition to follower role
 * 
 * @param term The current term
 * @param leader_id The ID of the current leader (empty if unknown)
 */
void RaftNode::become_follower(uint64_t term, const std::string& leader_id) {
    // Only log if actually changing role
    if (role_ != ServerRole::FOLLOWER) {
        std::cout << "Node " << node_id_ << " becoming FOLLOWER for term " << term << std::endl;
    }
    
    // Update role and leader
    role_ = ServerRole::FOLLOWER;
    current_leader_ = leader_id;
    
    // Reset election timeout
    reset_election_deadline();
}

/**
 * Transition to candidate role and start an election
 */
void RaftNode::become_candidate() {
    // Update state
    role_ = ServerRole::CANDIDATE;
    current_term_++;
    voted_for_ = node_id_; // Vote for self
    
    // Reset votes received
    votes_received_.clear();
    votes_received_.resize(peer_addresses_.size(), false);
    
    // Reset election timer
    last_heartbeat_ = std::chrono::steady_clock::now();
    
    std::cout << "Node " << node_id_ << " became candidate for term " << current_term_ << std::endl;
    
    // Request votes from all peers
    request_votes_from_all();
}

/**
 * Transition to leader role after winning an election
 */
void RaftNode::become_leader() {
    if (role_ == ServerRole::LEADER) {
        return; // Already leader
    }
    
    std::cout << "Node " << node_id_ << " became leader for term " << current_term_ << std::endl;
    
    // Update role
    role_ = ServerRole::LEADER;
    
    // Clear voted_for
    voted_for_ = "";
    
    // Update leader ID
    current_leader_ = node_id_;
    
    // Initialize leader state
    next_index_.clear();
    next_index_.resize(peer_addresses_.size(), log_.size());
    
    match_index_.clear();
    match_index_.resize(peer_addresses_.size(), 0);
    
    // Add an entry for the leader itself at the end of match_index_
    // This is used to count the leader's vote when calculating majority for commit
    match_index_.push_back(log_.size() - 1);
    
    // Send initial empty AppendEntries RPCs (heartbeats) to establish authority
    send_append_entries_to_all();
    
    // Reset heartbeat timer
    heartbeat_deadline_ = std::chrono::steady_clock::now() + heartbeat_interval_;
    
    // Notify the ServerRegistry of the leader change
    if (server_registry_) {
        server_registry_->update_leader_from_raft(node_id_, current_term_);
    }
}

/**
 * Send AppendEntries RPCs to all followers
 * 
 * @param heartbeat If true, send even if there are no new entries (heartbeat)
 */
void RaftNode::send_append_entries_to_all(bool heartbeat) {
    // Only the leader can send AppendEntries
    if (role_ != ServerRole::LEADER) {
        return;
    }
    
    std::cout << "Leader " << node_id_ << " sending " 
              << (heartbeat ? "heartbeat" : "AppendEntries") 
              << " to all followers" << std::endl;
    
    // Send to each peer
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        // Send the AppendEntries RPC
        send_append_entries(i);
    }
    
    // Update the heartbeat deadline
    heartbeat_deadline_ = std::chrono::steady_clock::now() + heartbeat_interval_;
}

/**
 * Send an AppendEntries RPC to a specific peer
 * 
 * @param peer_idx The index of the peer to send the RPC to
 */
void RaftNode::send_append_entries(size_t peer_idx) {
    // Prepare the arguments
    AppendEntriesArgs args;
    args.term = current_term_;
    args.leader_id = node_id_;
    
    // Include the index and term of the entry immediately preceding new ones
    args.prev_log_index = next_index_[peer_idx] - 1;
    args.prev_log_term = args.prev_log_index > 0 && args.prev_log_index < log_.size() 
                        ? log_[args.prev_log_index].term : 0;
    
    // Include the leader's commit index
    args.leader_commit = commit_index_;
    
    // Include entries starting from next_index
    size_t entries_start = next_index_[peer_idx];
    size_t entries_end = log_.size();
    
    // Log the AppendEntries RPC details
    std::string rpc_type = entries_start < entries_end ? "AppendEntries" : "Heartbeat";
    std::cout << "Leader " << node_id_ << " sending " << rpc_type << " to " 
              << peer_addresses_[peer_idx] << " (prev_index: " << args.prev_log_index 
              << ", prev_term: " << args.prev_log_term 
              << ", entries: " << (entries_end - entries_start) 
              << ", commit: " << args.leader_commit << ")" << std::endl;
    
    // Add entries to the request if there are any to send
    for (size_t i = entries_start; i < entries_end; ++i) {
        if (i < log_.size()) {  // Double-check to avoid out-of-bounds access
            args.entries.push_back(log_[i]);
            
            // Log details about the entry being sent
            std::string op_type = log_[i].cmd.has_insert() ? "INSERT" : 
                                 (log_[i].cmd.has_delete() ? "DELETE" : "UNKNOWN");
            
            std::cout << "Leader " << node_id_ << " sending entry " << i << " (term " 
                      << log_[i].term << ", type: " << op_type << ") to " 
                      << peer_addresses_[peer_idx] << std::endl;
        }
    }
    
    // Send the RPC
    AppendEntriesReply reply;
    bool success = send_append_entries_rpc(peer_idx, args, reply);
    
    if (success) {
        // Handle the reply
        handle_append_entries_reply(peer_idx, args, reply);
    } else {
        // If the RPC failed, log the error and retry later
        std::cerr << "Leader " << node_id_ << " failed to send AppendEntries to " 
                  << peer_addresses_[peer_idx] << std::endl;
    }
}

/**
 * Send an AppendEntries RPC to a specific peer and get the reply
 * 
 * @param peer_idx The index of the peer to send the RPC to
 * @param args The arguments to send
 * @param reply The reply to populate
 * @return True if the RPC was successful, false otherwise
 */
bool RaftNode::send_append_entries_rpc(size_t peer_idx, const AppendEntriesArgs& args, AppendEntriesReply& reply) {
    // Check if we have a gRPC stub for this peer
    if (!peer_stubs_[peer_idx]) {
        std::cerr << "Leader " << node_id_ << " failed to send AppendEntries to " 
                  << peer_addresses_[peer_idx] << ": stub not initialized" << std::endl;
        return false;
    }
    
    // If this is a heartbeat (no entries), use the simplified heartbeat format
    if (args.entries.empty()) {
        // Create a ReplicateMutationRequest to use as a proxy for AppendEntries
        csvservice::ReplicateMutationRequest request;
        request.set_filename("raft_heartbeat");
        
        // Encode the AppendEntries data in the first value
        auto* insert = request.mutable_row_insert();
        
        // Format: "term|prev_log_index|prev_log_term|leader_commit|leader_id"
        std::string encoded_data = std::to_string(args.term) + "|" +
                                  std::to_string(args.prev_log_index) + "|" +
                                  std::to_string(args.prev_log_term) + "|" +
                                  std::to_string(args.leader_commit) + "|" +
                                  args.leader_id;
        
        insert->add_values(encoded_data);
        
        // Create gRPC context with timeout
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
        
        // Response object
        csvservice::ReplicateMutationResponse response;
        
        // Send the RPC
        grpc::Status status = peer_stubs_[peer_idx]->ApplyMutation(&context, request, &response);
        
        // Handle the response
        if (status.ok()) {
            // Process the response
            reply.success = response.success();
            reply.term = args.term; // We don't have term in the response, so use the request term
            
            // Extract additional information from the response message if available
            if (!response.success() && response.message().find("Term mismatch:") == 0) {
                // Extract the term from the message
                std::string term_str = response.message().substr(14); // Skip "Term mismatch: "
                try {
                    reply.term = std::stoull(term_str);
                } catch (const std::exception& e) {
                    // Ignore parsing errors
                }
            }
            
            return true;
        } else {
            std::cerr << "Failed to send heartbeat to " << peer_addresses_[peer_idx] 
                      << ": " << status.error_message() << std::endl;
            return false;
        }
    } else {
        // This is a regular AppendEntries with entries
        
        // First, send a header with metadata
        csvservice::ReplicateMutationRequest header_request;
        header_request.set_filename("raft_append_entries_header");
        
        // Encode the AppendEntries data in the first value
        auto* header_insert = header_request.mutable_row_insert();
        
        // Format: "term|leader_id|prev_log_index|prev_log_term|leader_commit"
        std::string header_data = std::to_string(args.term) + "|" +
                                 args.leader_id + "|" +
                                 std::to_string(args.prev_log_index) + "|" +
                                 std::to_string(args.prev_log_term) + "|" +
                                 std::to_string(args.leader_commit);
        
        header_insert->add_values(header_data);
        
        // Create gRPC context with timeout
        grpc::ClientContext header_context;
        header_context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
        
        // Response object
        csvservice::ReplicateMutationResponse header_response;
        
        // Send the header RPC
        grpc::Status header_status = peer_stubs_[peer_idx]->ApplyMutation(&header_context, header_request, &header_response);
        
        // If the header RPC failed, return false
        if (!header_status.ok() || !header_response.success()) {
            std::cerr << "Failed to send AppendEntries header to " << peer_addresses_[peer_idx] 
                      << ": " << (header_status.ok() ? header_response.message() : header_status.error_message()) 
                      << std::endl;
            
            // Extract term from the response if available
            if (header_status.ok() && !header_response.success() && 
                header_response.message().find("Term mismatch:") == 0) {
                // Extract the term from the message
                std::string term_str = header_response.message().substr(14); // Skip "Term mismatch: "
                try {
                    reply.term = std::stoull(term_str);
                    reply.success = false;
                    return true; // We got a valid response, even though it's a failure
                } catch (const std::exception& e) {
                    // Ignore parsing errors
                }
            }
            
            return false;
        }
        
        // Now send each entry as a separate RPC
        bool all_success = true;
        
        for (const auto& entry : args.entries) {
            // Create a ReplicateMutationRequest for this entry
            csvservice::ReplicateMutationRequest entry_request;
            entry_request.set_filename(entry.cmd.file);
            
            // Add Raft metadata to the first value if it's an insert
            std::string raft_metadata = "RAFT|" + std::to_string(args.term) + "|" +
                                       args.leader_id + "|" +
                                       std::to_string(args.prev_log_index) + "|" +
                                       std::to_string(args.prev_log_term) + "|" +
                                       std::to_string(args.leader_commit);
            
            if (entry.cmd.has_insert()) {
                auto* insert = entry_request.mutable_row_insert();
                
                // Add the Raft metadata as the first value
                insert->add_values(raft_metadata);
                
                // Add the actual values
                for (const auto& value : entry.cmd.insert().values) {
                    insert->add_values(value);
                }
            } else if (entry.cmd.has_delete()) {
                auto* del = entry_request.mutable_row_delete();
                del->set_row_index(entry.cmd.del().row_index);
                
                // For delete operations, we need to add the Raft metadata in a different way
                // since there's no values field in the delete request
                // We'll use a special filename prefix to indicate this is a Raft delete
                entry_request.set_filename("RAFT_DELETE|" + entry.cmd.file + "|" + raft_metadata);
            }
            
            // Create gRPC context with timeout
            grpc::ClientContext entry_context;
            entry_context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
            
            // Response object
            csvservice::ReplicateMutationResponse entry_response;
            
            // Send the entry RPC
            grpc::Status entry_status = peer_stubs_[peer_idx]->ApplyMutation(&entry_context, entry_request, &entry_response);
            
            // If the entry RPC failed, set all_success to false
            if (!entry_status.ok() || !entry_response.success()) {
                std::cerr << "Failed to send entry to " << peer_addresses_[peer_idx] 
                          << ": " << (entry_status.ok() ? entry_response.message() : entry_status.error_message()) 
                          << std::endl;
                all_success = false;
                
                // Extract term from the response if available
                if (entry_status.ok() && !entry_response.success() && 
                    entry_response.message().find("Term mismatch:") == 0) {
                    // Extract the term from the message
                    std::string term_str = entry_response.message().substr(14); // Skip "Term mismatch: "
                    try {
                        reply.term = std::stoull(term_str);
                    } catch (const std::exception& e) {
                        // Ignore parsing errors
                    }
                }
                
                break;
            }
        }
        
        // Set the reply
        reply.success = all_success;
        reply.term = args.term; // Use the request term as the default
        
        return true;
    }
}

/**
 * Handle the response from an AppendEntries RPC
 * 
 * @param peer_idx The index of the peer that responded
 * @param args The arguments that were sent
 * @param reply The reply that was received
 */
void RaftNode::handle_append_entries_reply(size_t peer_idx, const AppendEntriesArgs& args, const AppendEntriesReply& reply) {
    // Lock to safely access shared state
    std::lock_guard<std::mutex> lock(mu_);
    
    // If we're not the leader anymore, ignore the reply
    if (role_ != ServerRole::LEADER) {
        std::cout << "Node " << node_id_ << " received AppendEntries reply but is no longer leader, ignoring" << std::endl;
        return;
    }
    
    // If the reply has a higher term, step down
    if (reply.term > current_term_) {
        std::cout << "Leader " << node_id_ << " received AppendEntries reply with higher term "
                  << reply.term << " > " << current_term_ << ", stepping down" << std::endl;
        step_down(reply.term);
        return;
    }
    
    // If the RPC was successful
    if (reply.success) {
        // Update nextIndex and matchIndex for the follower
        if (!args.entries.empty()) {
            // Calculate the new next_index and match_index values
            uint64_t new_next_index = args.prev_log_index + args.entries.size() + 1;
            uint64_t new_match_index = args.prev_log_index + args.entries.size();
            
            // Only update if the new values are higher (avoid race conditions with older RPCs)
            if (new_next_index > next_index_[peer_idx]) {
                next_index_[peer_idx] = new_next_index;
                
                std::cout << "Leader " << node_id_ << " updated next index for " 
                          << peer_addresses_[peer_idx] << " to " << next_index_[peer_idx] 
                          << " (prev_log_index: " << args.prev_log_index 
                          << ", entries: " << args.entries.size() << ")" << std::endl;
            }
            
            if (new_match_index > match_index_[peer_idx]) {
                match_index_[peer_idx] = new_match_index;
                
                std::cout << "Leader " << node_id_ << " updated match index for " 
                          << peer_addresses_[peer_idx] << " to " << match_index_[peer_idx] << std::endl;
                
                // Try to update commit_index
                update_commit_index();
            }
        } else {
            // This was a heartbeat or empty AppendEntries, but the follower accepted it
            // This means the follower's log is consistent up to prev_log_index
            if (args.prev_log_index > match_index_[peer_idx]) {
                match_index_[peer_idx] = args.prev_log_index;
                next_index_[peer_idx] = args.prev_log_index + 1;
                
                std::cout << "Leader " << node_id_ << " updated match index for " 
                          << peer_addresses_[peer_idx] << " to " << match_index_[peer_idx] 
                          << " based on successful heartbeat" << std::endl;
                
                // Try to update commit_index
                update_commit_index();
            }
        }
    } else {
        // If the follower rejected the AppendEntries, decrement nextIndex and retry
        if (reply.conflict_term > 0) {
            // Fast rollback based on conflict information
            // Find the last entry with the conflicting term
            size_t i = args.prev_log_index;
            while (i > 0 && i < log_.size() && log_[i].term != reply.conflict_term) {
                i--;
            }
            
            if (i > 0 && i < log_.size() && log_[i].term == reply.conflict_term) {
                // Found an entry with the conflicting term, try from the next one
                next_index_[peer_idx] = i + 1;
            } else {
                // Didn't find an entry with the conflicting term, use conflict_index
                next_index_[peer_idx] = reply.conflict_index;
            }
            
            std::cout << "Leader " << node_id_ << " received conflict info from " 
                      << peer_addresses_[peer_idx] << ", updated next_index to " 
                      << next_index_[peer_idx] << std::endl;
        } else {
            // Simple rollback - decrement nextIndex
            if (next_index_[peer_idx] > 1) {
                next_index_[peer_idx]--;
                
                std::cout << "Leader " << node_id_ << " decremented next_index for " 
                          << peer_addresses_[peer_idx] << " to " << next_index_[peer_idx] << std::endl;
            }
        }
        
        // Retry immediately with the updated next_index
        send_append_entries(peer_idx);
    }
}

/**
 * Handle an incoming AppendEntries RPC
 * 
 * @param args The arguments of the RPC
 * @param reply The reply to send back
 */
void RaftNode::handle_append_entries(const AppendEntriesArgs& args, AppendEntriesReply& reply) {
    std::lock_guard<std::mutex> lock(mu_);
    
    // Reply false if term < currentTerm (§5.1)
    if (args.term < current_term_) {
        reply.term = current_term_;
        reply.success = false;
        reply.conflict_index = 0;
        reply.conflict_term = 0;
        
        std::cout << "Node " << node_id_ << " rejected AppendEntries from " << args.leader_id
                  << ": term " << args.term << " < current term " << current_term_ << std::endl;
        return;
    }
    
    // If we get a newer term, update our term and step down
    if (args.term > current_term_) {
        std::cout << "Node " << node_id_ << " received AppendEntries with higher term "
                  << args.term << " > " << current_term_ << ", stepping down" << std::endl;
        step_down(args.term);
    }
    
    // Reset election timer since we heard from the leader
    last_heartbeat_ = std::chrono::steady_clock::now();
    
    // Update leader ID
    current_leader_ = args.leader_id;
    
    // If we're a candidate, step down to follower
    if (role_ == ServerRole::CANDIDATE) {
        std::cout << "Node " << node_id_ << " was candidate but received valid AppendEntries, "
                  << "becoming follower" << std::endl;
        become_follower(current_term_, args.leader_id);
    }
    
    // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
    if (args.prev_log_index >= log_.size() || 
        (args.prev_log_index > 0 && 
         log_[args.prev_log_index].term != args.prev_log_term)) {
        
        reply.term = current_term_;
        reply.success = false;
        
        // Optimization: Include conflict information for faster log recovery
        if (args.prev_log_index >= log_.size()) {
            // We don't have the entry at prev_log_index
            reply.conflict_index = log_.size();
            reply.conflict_term = 0;
            
            std::cout << "Node " << node_id_ << " rejected AppendEntries: missing entry at index "
                      << args.prev_log_index << " (log size: " << log_.size() << ")" << std::endl;
        } else {
            // We have the entry but terms don't match
            reply.conflict_term = log_[args.prev_log_index].term;
            
            // Find the first index with that term
            reply.conflict_index = args.prev_log_index;
            while (reply.conflict_index > 1 && 
                   log_[reply.conflict_index - 1].term == reply.conflict_term) {
                reply.conflict_index--;
            }
            
            std::cout << "Node " << node_id_ << " rejected AppendEntries: term mismatch at index "
                      << args.prev_log_index << " (expected term " << args.prev_log_term
                      << ", got term " << log_[args.prev_log_index].term << ")" << std::endl;
        }
        
        return;
    }
    
    // If we get here, the AppendEntries RPC is valid
    std::cout << "Node " << node_id_ << " processing valid AppendEntries from " << args.leader_id
              << " (prev_log_index: " << args.prev_log_index
              << ", entries: " << args.entries.size()
              << ", leader_commit: " << args.leader_commit << ")" << std::endl;
    
    // If existing entries conflict with new entries, delete all existing entries starting with first conflicting entry (§5.3)
    // Then append any new entries not already in the log
    size_t new_entries_idx = 0;
    size_t log_idx = args.prev_log_index + 1;
    
    // Skip entries that are already in our log with the same term
    while (new_entries_idx < args.entries.size() && log_idx < log_.size()) {
        if (log_[log_idx].term != args.entries[new_entries_idx].term) {
            // Found a conflict, truncate log here
            std::cout << "Node " << node_id_ << " found conflicting entry at index " << log_idx
                      << " (log term: " << log_[log_idx].term << ", entry term: "
                      << args.entries[new_entries_idx].term << "), truncating log" << std::endl;
            log_.resize(log_idx);
            break;
        }
        
        new_entries_idx++;
        log_idx++;
    }
    
    // Append any new entries not already in the log
    for (size_t i = new_entries_idx; i < args.entries.size(); i++) {
        if (log_idx < log_.size()) {
            // Replace existing entry
            log_[log_idx] = args.entries[i];
            std::cout << "Node " << node_id_ << " replaced entry at index " << log_idx 
                      << " (term: " << args.entries[i].term << ")" << std::endl;
        } else {
            // Append new entry
            log_.push_back(args.entries[i]);
            std::cout << "Node " << node_id_ << " appended new entry at index " << log_idx 
                      << " (term: " << args.entries[i].term << ")" << std::endl;
        }
        log_idx++;
    }
    
    // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (§5.3)
    if (args.leader_commit > commit_index_) {
        uint64_t old_commit_index = commit_index_;
        size_t last_new_entry = args.prev_log_index + args.entries.size();
        commit_index_ = std::min<uint64_t>(args.leader_commit, last_new_entry);
        
        if (commit_index_ > old_commit_index) {
            std::cout << "Node " << node_id_ << " updated commit index from " << old_commit_index
                      << " to " << commit_index_ << std::endl;
            
            // Notify the apply thread that there are new entries to apply
            apply_cv_.notify_one();
        }
    }
    
    // Reply success
    reply.term = current_term_;
    reply.success = true;
    reply.conflict_index = 0;
    reply.conflict_term = 0;
    
    std::cout << "Node " << node_id_ << " accepted AppendEntries from " << args.leader_id
              << " (prev_log_index: " << args.prev_log_index
              << ", entries: " << args.entries.size() << ")" << std::endl;
}

/**
 * Handle an incoming AppendEntries RPC
 * 
 * @param request The gRPC request
 * @param response The gRPC response to send back
 */
void RaftNode::handle_append_entries_rpc(const csvservice::ReplicateMutationRequest& request,
                                       csvservice::ReplicateMutationResponse& response) {
    // Check if this is a Raft AppendEntries proxy request
    if (request.filename() == "raft_heartbeat") {
        // This is a heartbeat - decode the AppendEntries data from the values
        if (request.has_row_insert() && !request.row_insert().values().empty()) {
            std::string encoded_data = request.row_insert().values(0);
            
            // Parse the encoded data
            // Format: "term|prev_log_index|prev_log_term|leader_commit"
            std::vector<std::string> parts;
            size_t pos = 0;
            std::string token;
            std::string delimiter = "|";
            std::string data_copy = encoded_data;
            
            while ((pos = data_copy.find(delimiter)) != std::string::npos) {
                token = data_copy.substr(0, pos);
                parts.push_back(token);
                data_copy.erase(0, pos + delimiter.length());
            }
            parts.push_back(data_copy); // Add the last part
            
            // Ensure we have all the parts
            if (parts.size() >= 4) {
                // Create AppendEntries arguments
                AppendEntriesArgs args;
                args.term = std::stoull(parts[0]);
                args.leader_id = parts.size() >= 5 ? parts[4] : "unknown"; // Use leader_id if provided
                args.prev_log_index = std::stoull(parts[1]);
                args.prev_log_term = std::stoull(parts[2]);
                args.leader_commit = std::stoull(parts[3]);
                
                // Process the heartbeat
                AppendEntriesReply reply;
                handle_append_entries_safe(args, reply);
                
                // Set the response
                response.set_success(reply.success);
                std::string error_msg = "Failed to append entry: ";
                error_msg += (reply.term > args.term ? "Higher term" : "Log inconsistency");
                response.set_message(reply.success ? "Success" : error_msg);
                
                // If we're rejecting due to term, include that in the message
                if (!reply.success && reply.term > args.term) {
                    response.set_message("Term mismatch: " + std::to_string(reply.term));
                }
            } else {
                // Invalid format
                response.set_success(false);
                response.set_message("Invalid heartbeat format");
            }
        } else {
            // Invalid format
            response.set_success(false);
            response.set_message("Invalid heartbeat request");
        }
    } else if (request.has_row_insert() || request.has_row_delete()) {
        // This is a regular AppendEntries RPC with actual entries
        
        // First, check if this is a special AppendEntries header with metadata
        if (request.filename() == "raft_append_entries_header" && request.has_row_insert()) {
            // Extract metadata from the header
            std::string encoded_data = request.row_insert().values(0);
            
            // Parse the encoded data
            // Format: "term|leader_id|prev_log_index|prev_log_term|leader_commit"
            std::vector<std::string> parts;
            size_t pos = 0;
            std::string token;
            std::string delimiter = "|";
            std::string data_copy = encoded_data;
            
            while ((pos = data_copy.find(delimiter)) != std::string::npos) {
                token = data_copy.substr(0, pos);
                parts.push_back(token);
                data_copy.erase(0, pos + delimiter.length());
            }
            parts.push_back(data_copy); // Add the last part
            
            // Ensure we have all the parts
            if (parts.size() >= 5) {
                // Create AppendEntries arguments
                AppendEntriesArgs args;
                args.term = std::stoull(parts[0]);
                args.leader_id = parts[1];
                args.prev_log_index = std::stoull(parts[2]);
                args.prev_log_term = std::stoull(parts[3]);
                args.leader_commit = std::stoull(parts[4]);
                
                // Process the AppendEntries request
                AppendEntriesReply reply;
                handle_append_entries_safe(args, reply);
                
                // Set the response
                response.set_success(reply.success);
                if (reply.success) {
                    response.set_message("AppendEntries header processed successfully");
                } else {
                    std::string error_msg = "Failed to process AppendEntries header: ";
                    error_msg += (reply.term > args.term ? "Higher term" : "Log inconsistency");
                    response.set_message(error_msg);
                }
            } else {
                // Invalid format
                response.set_success(false);
                response.set_message("Invalid AppendEntries header format");
            }
            return;
        }
        
        // Create a mutation from the request
        Mutation mutation;
        mutation.file = request.filename();
        
        if (request.has_row_insert()) {
            std::vector<std::string> values;
            for (const auto& value : request.row_insert().values()) {
                values.push_back(value);
            }
            mutation.op = RowInsert{values};
        } else if (request.has_row_delete()) {
            mutation.op = RowDelete{static_cast<int>(request.row_delete().row_index())};
        }
        
        // Extract term, leader, and other metadata from the request
        // Look for a special value in the format "RAFT|term|leader_id|prev_log_index|prev_log_term|leader_commit"
        uint64_t term = current_term_;
        std::string leader_id = current_leader_;
        uint64_t prev_log_index = 0;
        uint64_t prev_log_term = 0;
        uint64_t leader_commit = commit_index_;
        
        if (request.has_row_insert() && !request.row_insert().values().empty()) {
            const std::string& first_value = request.row_insert().values(0);
            if (first_value.find("RAFT|") == 0) {
                // This is a special value with Raft metadata
                std::string metadata = first_value.substr(5); // Skip "RAFT|"
                
                // Parse the metadata
                // Format: "term|leader_id|prev_log_index|prev_log_term|leader_commit"
                std::vector<std::string> parts;
                size_t pos = 0;
                std::string token;
                std::string delimiter = "|";
                std::string data_copy = metadata;
                
                while ((pos = data_copy.find(delimiter)) != std::string::npos) {
                    token = data_copy.substr(0, pos);
                    parts.push_back(token);
                    data_copy.erase(0, pos + delimiter.length());
                }
                parts.push_back(data_copy); // Add the last part
                
                // Extract the metadata
                if (parts.size() >= 5) {
                    term = std::stoull(parts[0]);
                    leader_id = parts[1];
                    prev_log_index = std::stoull(parts[2]);
                    prev_log_term = std::stoull(parts[3]);
                    leader_commit = std::stoull(parts[4]);
                }
            }
        }
        
        // Create AppendEntries arguments with this entry
        AppendEntriesArgs args;
        {
            std::lock_guard<std::mutex> lock(mu_);
            args.term = term;
            args.leader_id = leader_id;
            args.prev_log_index = prev_log_index;
            args.prev_log_term = prev_log_term;
            args.leader_commit = leader_commit;
            
            // Create a log entry for this mutation
            LogEntry entry;
            entry.term = term;
            entry.cmd = mutation;
            args.entries.push_back(entry);
        }
        
        // Process the AppendEntries request
        AppendEntriesReply reply;
        handle_append_entries_safe(args, reply);
        
        // Set the response
        response.set_success(reply.success);
        if (reply.success) {
            response.set_message("Entry appended successfully");
        } else {
            std::string error_msg = "Failed to append entry: ";
            error_msg += (reply.term > args.term ? "Higher term" : "Log inconsistency");
            response.set_message(error_msg);
        }
    } else {
        // Invalid request
        response.set_success(false);
        response.set_message("Invalid AppendEntries request");
    }
}

/**
 * Request votes from all peers
 */
void RaftNode::request_votes_from_all() {
    // Prepare RequestVote arguments
    RequestVoteArgs args;
    args.term = current_term_;
    args.candidate_id = node_id_;
    
    // Set last log index and term
    size_t last_log_idx = log_.size() - 1;
    args.last_log_index = last_log_idx;
    args.last_log_term = last_log_idx > 0 ? log_[last_log_idx].term : 0;
    
    // Send RequestVote RPCs to all peers
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        // Create a copy of the args to capture in the lambda
        auto args_copy = args;
        
        // Use async to avoid blocking
        std::thread([this, i, args_copy]() {
            // Create gRPC request for RegisterPeer as a proxy for RequestVote
            csvservice::RegisterPeerRequest proxy_request;
            proxy_request.set_peer_address(args_copy.candidate_id);
            
            // Add extra info in the peer address to encode our vote request
            // Format: candidate_id|term|last_log_index|last_log_term
            std::string encoded_vote_request = args_copy.candidate_id + "|" + 
                                              std::to_string(args_copy.term) + "|" +
                                              std::to_string(args_copy.last_log_index) + "|" +
                                              std::to_string(args_copy.last_log_term);
            proxy_request.set_peer_address(encoded_vote_request);
            
            // Create gRPC context with timeout
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
            
            // Response object
            csvservice::RegisterPeerResponse proxy_response;
            
            // Send the RPC
            grpc::Status status;
            if (peer_stubs_[i]) {
                status = peer_stubs_[i]->RegisterPeer(&context, proxy_request, &proxy_response);
            }
            
            // Process the response
            if (status.ok()) {
                // Convert gRPC response to our internal format
                RequestVoteReply reply;
                reply.term = current_term_; // Assume same term for now
                reply.vote_granted = proxy_response.success();
                
                // Handle the reply
                std::lock_guard<std::mutex> lock(mu_);
                handle_request_vote_reply(i, args_copy, reply);
            } else {
                std::cout << "Failed to send RequestVote to " << peer_addresses_[i] 
                          << ": " << status.error_message() << std::endl;
            }
        }).detach();
    }
}

/**
 * Handle an incoming RequestVote RPC
 * 
 * @param args The arguments of the RPC
 * @param reply The reply to send back
 */
void RaftNode::handle_request_vote(const RequestVoteArgs& args, RequestVoteReply& reply) {
    // Initialize reply with current term and false vote
    reply.term = current_term_;
    reply.vote_granted = false;
    
    // Reply false if term < currentTerm (§5.1)
    if (args.term < current_term_) {
        return;
    }
    
    // If we get a newer term, update our term and step down
    if (args.term > current_term_) {
        step_down(args.term);
    }
    
    // If we've already voted for someone else in this term, deny the vote
    if (voted_for_ != "" && voted_for_ != args.candidate_id && current_term_ == args.term) {
        return;
    }
    
    // Check if candidate's log is at least as up-to-date as ours (§5.4.1)
    size_t last_log_idx = log_.size() - 1;
    uint64_t last_log_term = last_log_idx > 0 ? log_[last_log_idx].term : 0;
    
    if (args.last_log_term < last_log_term || 
        (args.last_log_term == last_log_term && args.last_log_index < last_log_idx)) {
        // Our log is more up-to-date, deny the vote
        return;
    }
    
    // Grant vote
    voted_for_ = args.candidate_id;
    reply.vote_granted = true;
    
    // Reset election timer since we just voted
    last_heartbeat_ = std::chrono::steady_clock::now();
}

/**
 * This method is called by the gRPC service implementation when it receives a RequestVote RPC
 * 
 * @param request The gRPC request
 * @param response The gRPC response to send back
 */
void RaftNode::handle_request_vote_rpc(const csvservice::RequestVoteRequest& request,
                                     csvservice::RequestVoteResponse& response) {
    // Convert gRPC request to our internal format
    RequestVoteArgs args;
    args.term = request.term();
    args.candidate_id = request.candidate_id();
    args.last_log_index = request.last_log_index();
    args.last_log_term = request.last_log_term();
    
    // Process the request
    RequestVoteReply reply;
    {
        std::lock_guard<std::mutex> lock(mu_);
        handle_request_vote(args, reply);
    }
    
    // Convert our reply to gRPC response
    response.set_term(reply.term);
    response.set_vote_granted(reply.vote_granted);
}

/**
 * Handle a reply to a RequestVote RPC
 * 
 * @param peer_idx The index of the peer that sent the reply
 * @param args The arguments of the original RPC
 * @param reply The reply received
 */
void RaftNode::handle_request_vote_reply(int peer_idx, const RequestVoteArgs& args, const RequestVoteReply& reply) {
    // If we're not a candidate anymore, ignore the reply
    if (role_ != ServerRole::CANDIDATE) {
        return;
    }
    
    // If the reply has a higher term, update our term and step down
    if (reply.term > current_term_) {
        step_down(reply.term);
        return;
    }
    
    // If the reply is for an older term, ignore it
    if (reply.term < current_term_) {
        return;
    }
    
    // If the vote was granted, count it
    if (reply.vote_granted) {
        votes_received_[peer_idx] = true;
        
        // Count total votes received
        int votes = 1; // Include our self-vote
        for (bool vote : votes_received_) {
            if (vote) votes++;
        }
        
        // If we have a majority, become leader
        if (votes > (peer_addresses_.size() + 1) / 2) {
            become_leader();
        }
    }
}

/**
 * Update the commit index based on the match indices of followers
 */
void RaftNode::update_commit_index() {
    // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, 
    // and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
    uint64_t old_commit_index = commit_index_;
    
    // Start from the next index after the current commit index
    for (uint64_t n = commit_index_ + 1; n < log_.size(); ++n) {
        // Only consider entries from current term
        // This is a critical Raft safety property - we can only commit entries from our term
        if (log_[n].term != current_term_) {
            std::cout << "Leader " << node_id_ << " skipping entry " << n 
                      << " for commit: term " << log_[n].term 
                      << " != current term " << current_term_ << std::endl;
            continue;
        }
        
        // Count replications (including the leader itself)
        int replications = 1;  // Start with 1 for the leader itself
        for (size_t i = 0; i < match_index_.size() - 1; ++i) {  // Exclude the leader's entry at the end
            if (match_index_[i] >= n) {
                replications++;
            }
        }
        
        // Calculate the majority threshold
        // Total nodes = peer_addresses_.size() + 1 (including the leader)
        int majority = (peer_addresses_.size() + 1) / 2 + 1;
        
        std::cout << "Leader " << node_id_ << " checking entry " << n 
                  << " for commit: replicated on " << replications 
                  << " servers, majority is " << majority << std::endl;
        
        // If a majority of servers have replicated the entry, commit it
        if (replications >= majority) {
            commit_index_ = n;
            std::cout << "Leader " << node_id_ << " updated commit index to " << n 
                      << " (replicated on " << replications << " servers, majority is " 
                      << majority << ")" << std::endl;
        } else {
            // If we can't commit this entry, we can't commit any later entries either
            break;
        }
    }
    
    // If commit index changed, notify apply thread
    if (commit_index_ > old_commit_index) {
        std::cout << "Leader " << node_id_ << " commit index changed from " << old_commit_index
                  << " to " << commit_index_ << std::endl;
        
        // Notify the apply thread that there are new entries to apply
        apply_cv_.notify_one();
        
        // Send AppendEntries to all followers to update their commit indices
        send_append_entries_to_all(false);
    }
}

/**
 * Apply committed entries to the state machine
 */
void RaftNode::apply_entries() {
    // Apply all entries between last_applied and commit_index
    uint64_t old_last_applied = last_applied_;
    
    while (last_applied_ < commit_index_) {
        last_applied_++;
        
        // Make sure the entry exists
        if (last_applied_ < log_.size()) {
            // Get the entry to apply
            const LogEntry& entry = log_[last_applied_];
            
            // Apply the entry to the state machine
            try {
                // Log details about the entry being applied
                std::string op_type;
                if (entry.cmd.has_insert()) {
                    op_type = "INSERT";
                    std::cout << "Node " << node_id_ << " applying INSERT entry " << last_applied_ 
                              << " (term " << entry.term << ") to state machine for file " 
                              << entry.cmd.file << " with " << entry.cmd.insert().values.size() 
                              << " values" << std::endl;
                } else if (entry.cmd.has_delete()) {
                    op_type = "DELETE";
                    std::cout << "Node " << node_id_ << " applying DELETE entry " << last_applied_ 
                              << " (term " << entry.term << ") to state machine for file " 
                              << entry.cmd.file << " at row " << entry.cmd.del().row_index << std::endl;
                } else {
                    op_type = "UNKNOWN";
                    std::cout << "Node " << node_id_ << " applying UNKNOWN entry " << last_applied_ 
                              << " (term " << entry.term << ") to state machine for file " 
                              << entry.cmd.file << std::endl;
                }
                
                // Actually apply the entry to the state machine
                state_machine_->apply(entry.cmd);
                
                std::cout << "Node " << node_id_ << " successfully applied " << op_type << " entry " 
                          << last_applied_ << " (term " << entry.term 
                          << ") to state machine for file " << entry.cmd.file << std::endl;
            } catch (const std::exception& e) {
                std::cerr << "Node " << node_id_ << " failed to apply entry " << last_applied_
                          << " to state machine: " << e.what() << std::endl;
                // We still increment last_applied_ even if application fails
                // This is to avoid getting stuck in an infinite loop if an entry is unapplicable
            }
        } else {
            std::cerr << "Node " << node_id_ << " tried to apply entry " << last_applied_
                      << " but it doesn't exist in the log (log size: " << log_.size() << ")" << std::endl;
        }
    }
    
    // If we applied any entries and we're the leader, send AppendEntries to followers
    // This helps followers update their commit indices more quickly
    if (last_applied_ > old_last_applied) {
        std::cout << "Node " << node_id_ << " applied entries from " << old_last_applied + 1 
                  << " to " << last_applied_ << std::endl;
        
        if (role_ == ServerRole::LEADER) {
            std::cout << "Leader " << node_id_ << " sending AppendEntries to followers "
                      << "after applying entries " << old_last_applied + 1 << " to " << last_applied_ << std::endl;
            
            // Use non-heartbeat AppendEntries to ensure followers get the updated commit index
            send_append_entries_to_all(false);
        }
    }
}

/**
 * Reset the election timeout with a random value
 */
void RaftNode::reset_election_deadline() {
    // Generate a random timeout between 150ms and 300ms
    std::uniform_int_distribution<> dist(150, 300);
    election_deadline_ = std::chrono::steady_clock::now() + 
                         std::chrono::milliseconds(dist(rng_));
}

/**
 * Check if a candidate's log is at least as up-to-date as ours
 * 
 * @param last_log_index The index of the candidate's last log entry
 * @param last_log_term The term of the candidate's last log entry
 * @return True if the candidate's log is at least as up-to-date as ours
 */
bool RaftNode::is_log_up_to_date(uint64_t last_log_index, uint64_t last_log_term) {
    uint64_t my_last_term = get_last_log_term();
    
    // If the terms are different, the higher term is more up-to-date
    if (last_log_term != my_last_term) {
        return last_log_term > my_last_term;
    }
    
    // If the terms are the same, the longer log is more up-to-date
    return last_log_index >= get_last_log_index();
}

/**
 * Get the index of the last log entry
 * 
 * @return The index of the last log entry
 */
uint64_t RaftNode::get_last_log_index() const {
    return log_.size() - 1;
}

/**
 * Get the term of the last log entry
 * 
 * @return The term of the last log entry
 */
uint64_t RaftNode::get_last_log_term() const {
    if (log_.empty()) {
        return 0;
    }
    return log_[log_.size() - 1].term;
}

/**
 * Set the server registry for leader updates
 * 
 * @param registry Pointer to the ServerRegistry
 */
void RaftNode::set_server_registry(network::ServerRegistry* registry) {
    std::lock_guard<std::mutex> lock(mu_);
    server_registry_ = registry;
    std::cout << "RaftNode: ServerRegistry set successfully" << std::endl;
}
