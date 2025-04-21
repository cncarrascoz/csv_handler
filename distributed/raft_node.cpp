#include "raft_node.hpp"
#include "rpc/SocketRaftRPC.hpp"
#include <iostream>
#include <random>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

RaftNode::RaftNode(
    const std::string& node_id,
    std::shared_ptr<raft::RaftStateMachine> state_machine,
    const std::vector<std::string>& peer_addresses,
    const std::string& storage_dir)
    : node_id_(node_id),
      state_machine_(state_machine),
      peer_addresses_(peer_addresses),
      storage_dir_(storage_dir),
      next_index_(peer_addresses_.size(), 1),
      match_index_(peer_addresses_.size(), 0),
      random_engine_(std::random_device()()) {
    
    // Initialize with empty indices for each peer
    //next_index_.resize(peer_addresses_.size(), 1);
    //match_index_.resize(peer_addresses_.size(), 0);
    
    // Set initial election timeout
    reset_election_timeout();
    
    // Create the storage directory if it doesn't exist
    if (!storage_dir_.empty()) {
        std::filesystem::create_directories(storage_dir_);
    }
    
    // Load persistent state if available
    load_state();
    
    // Create the RPC client
    rpc_ = std::make_unique<SocketRaftRPC>(
        node_id_, 
        [this](const RequestVoteRequest& req) { return this->handle_request_vote(req); },
        [this](const AppendEntriesRequest& req) { return this->handle_append_entries(req); }
    );
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }
    
    // Start the RPC server
    rpc_->start();
    
    // Start in follower state if we have peers, otherwise standalone
    if (peer_addresses_.empty()) {
        role_ = ServerRole::STANDALONE;
        std::cout << "Node " << node_id_ << " starting in STANDALONE mode" << std::endl;
    } else {
        role_ = ServerRole::FOLLOWER;
        std::cout << "Node " << node_id_ << " starting in FOLLOWER mode" << std::endl;
        
        // Start the ticker thread for heartbeats, elections, etc.
        ticker_thread_ = std::thread(&RaftNode::tick, this);
        
        // Start the apply thread for applying committed entries
        apply_thread_ = std::thread(&RaftNode::apply_entries, this);
    }
}

void RaftNode::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    // Stop the RPC server
    rpc_->stop();
    
    // Wait for the ticker thread to finish
    if (ticker_thread_.joinable()) {
        ticker_thread_.join();
    }
    
    // Notify and wait for the apply thread to finish
    apply_cv_.notify_all();
    if (apply_thread_.joinable()) {
        apply_thread_.join();
    }
    
    std::cout << "Node " << node_id_ << " stopped" << std::endl;
}

bool RaftNode::submit(const raft::Mutation& mutation) {
    // In STANDALONE mode, apply directly
    if (role_ == ServerRole::STANDALONE) {
        state_machine_->apply(mutation);
        return true;
    }
    
    // If not leader, forward to leader or reject
    if (role_ != ServerRole::LEADER) {
        if (!current_leader_.empty() && current_leader_ != node_id_) {
            std::cout << "Forwarding mutation to leader: " << current_leader_ << std::endl;
            // In a real implementation, we would forward the mutation to the leader
            return false;
        }
        
        std::cout << "Cannot submit mutation: not the leader" << std::endl;
        return false;
    }
    
    // Append to local log
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint64_t last_index = get_last_log_index();
    uint64_t new_index = last_index + 1;
    uint64_t current_term = current_term_;
    
    raft::LogEntry entry(current_term, new_index, mutation);
    log_.push_back(entry);
    
    // Save state
    save_state();
    
    std::cout << "Leader " << node_id_ << " appended mutation to log at index " << new_index << std::endl;
    
    // Replicate to followers (this happens in the ticker thread)
    return true;
}

ServerRole RaftNode::role() const {
    return role_;
}

uint64_t RaftNode::current_term() const {
    return current_term_;
}

std::string RaftNode::current_leader() const {
    return current_leader_;
}

size_t RaftNode::cluster_size() const {
    return peer_addresses_.size() + 1; // +1 for self
}

size_t RaftNode::quorum_size() const {
    return (cluster_size() / 2) + 1;
}

std::vector<std::string> RaftNode::peer_addresses() const {
    return peer_addresses_;
}

std::vector<raft::LogEntry> RaftNode::log_entries() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return log_;
}

uint64_t RaftNode::commit_index() const {
    return commit_index_;
}

uint64_t RaftNode::last_applied() const {
    return last_applied_;
}

void RaftNode::tick() {
    while (running_) {
        auto role = role_.load();
        auto now = std::chrono::steady_clock::now();
        
        switch (role) {
            case ServerRole::FOLLOWER: {
                // Check if election timeout has elapsed
                if (now > last_heartbeat_ + election_timeout_) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    become_candidate();
                }
                break;
            }
            case ServerRole::CANDIDATE: {
                // Check if election timeout has elapsed
                if (now > last_heartbeat_ + election_timeout_) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    // Start a new election
                    become_candidate();
                }
                break;
            }
            case ServerRole::LEADER: {
                // Send heartbeats to followers
                send_heartbeats();
                break;
            }
            case ServerRole::STANDALONE:
                // Nothing to do
                break;
        }
        
        // Sleep for a short time
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void RaftNode::apply_entries() {
    uint64_t last_applied = last_applied_;
    
    while (running_) {
        uint64_t commit_index = commit_index_;
        
        if (last_applied < commit_index) {
            std::vector<raft::LogEntry> entries_to_apply;
            
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (uint64_t i = last_applied + 1; i <= commit_index && i <= log_.size(); ++i) {
                    entries_to_apply.push_back(log_[i - 1]); // Log is 0-indexed, but log entries are 1-indexed
                }
            }
            
            // Apply each entry to the state machine
            for (const auto& entry : entries_to_apply) {
                state_machine_->apply(entry.mutation());
                last_applied = entry.index();
                last_applied_ = last_applied;
                
                std::cout << "Node " << node_id_ << " applied entry " << entry.index() 
                          << " to state machine" << std::endl;
            }
        }
        
        // Wait for new entries to be committed
        std::unique_lock<std::mutex> lock(mutex_);
        apply_cv_.wait_for(lock, std::chrono::milliseconds(100), [this, last_applied] {
            return !running_ || commit_index_ > last_applied;
        });
    }
}

void RaftNode::step_down(uint64_t term) {
    if (term > current_term_) {
        current_term_ = term;
        voted_for_ = "";
        become_follower(term, "");
        
        // Save state
        save_state();
    }
}

void RaftNode::become_follower(uint64_t term, const std::string& leader_id) {
    role_ = ServerRole::FOLLOWER;
    current_leader_ = leader_id;
    reset_election_timeout();
    
    std::cout << "Node " << node_id_ << " is now a follower (term " << term << ")" << std::endl;
}

void RaftNode::become_candidate() {
    current_term_++;
    role_ = ServerRole::CANDIDATE;
    voted_for_ = node_id_; // Vote for self
    current_leader_ = "";
    reset_election_timeout();
    
    std::cout << "Node " << node_id_ << " is now a candidate (term " << current_term_ << ")" << std::endl;
    
    // Save state
    save_state();
    
    // Request votes from all peers
    uint64_t votes = 1; // Vote for self
    uint64_t term = current_term_;
    uint64_t last_log_index = get_last_log_index();
    uint64_t last_log_term = get_last_log_term();
    
    // Create the request
    RequestVoteRequest request;
    request.candidate_id = node_id_;
    request.term = term;
    request.last_log_index = last_log_index;
    request.last_log_term = last_log_term;
    
    // Send to all peers
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        const auto& peer = peer_addresses_[i];
        
        // Send the request in a separate thread
        std::thread([this, peer, request, &votes, term]() {
            try {
                RequestVoteResponse response = rpc_->request_vote(peer, request);
                
                // Process the response
                std::lock_guard<std::mutex> lock(mutex_);
                
                // If we've already moved on to a new term, ignore the response
                if (current_term_ != term || role_ != ServerRole::CANDIDATE) {
                    return;
                }
                
                // If the response term is higher than ours, step down
                if (response.term > current_term_) {
                    step_down(response.term);
                    return;
                }
                
                // If the vote was granted, increment the vote count
                if (response.vote_granted) {
                    votes++;
                    
                    // If we have a majority, become leader
                    if (votes >= quorum_size()) {
                        become_leader();
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "Error requesting vote from " << peer << ": " << e.what() << std::endl;
            }
        }).detach();
    }
}

void RaftNode::become_leader() {
    if (role_ != ServerRole::CANDIDATE) {
        return;
    }
    
    role_ = ServerRole::LEADER;
    current_leader_ = node_id_;
    
    // Initialize leader state
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        next_index_[i] = get_last_log_index() + 1;
        match_index_[i] = 0;
    }
    
    std::cout << "Node " << node_id_ << " is now the leader (term " << current_term_ << ")" << std::endl;
    
    // Append a no-op entry to the log
    raft::Mutation noop(raft::MutationType::NOOP, "");
    submit(noop);
    
    // Send initial heartbeats
    send_heartbeats();
}

RequestVoteResponse RaftNode::handle_request_vote(const RequestVoteRequest& request) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    RequestVoteResponse response;
    response.term = current_term_;
    response.vote_granted = false;
    
    // If the request term is less than our current term, reject
    if (request.term < current_term_) {
        return response;
    }
    
    // If the request term is greater than our current term, step down
    if (request.term > current_term_) {
        step_down(request.term);
    }
    
    // Check if we've already voted for someone else in this term
    if (voted_for_.empty() || voted_for_ == request.candidate_id) {
        // Check if the candidate's log is at least as up-to-date as ours
        uint64_t last_log_index = get_last_log_index();
        uint64_t last_log_term = get_last_log_term();
        
        if (request.last_log_term > last_log_term ||
            (request.last_log_term == last_log_term && request.last_log_index >= last_log_index)) {
            // Grant vote
            voted_for_ = request.candidate_id;
            response.vote_granted = true;
            
            // Save state
            save_state();
            
            // Reset election timeout
            reset_election_timeout();
            
            std::cout << "Node " << node_id_ << " voted for " << request.candidate_id 
                      << " in term " << request.term << std::endl;
        }
    }
    
    return response;
}

AppendEntriesResponse RaftNode::handle_append_entries(const AppendEntriesRequest& request) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    AppendEntriesResponse response;
    response.term = current_term_;
    response.success = false;
    response.match_index = 0;
    
    // If the request term is less than our current term, reject
    if (request.term < current_term_) {
        return response;
    }
    
    // If the request term is greater than our current term, step down
    if (request.term > current_term_) {
        step_down(request.term);
    }
    
    // This is a valid AppendEntries from the current leader
    reset_election_timeout();
    become_follower(request.term, request.leader_id);
    
    // Check if the previous log entry matches
    if (request.prev_log_index > 0) {
        if (request.prev_log_index > log_.size()) {
            // We don't have the previous log entry
            return response;
        }
        
        if (log_[request.prev_log_index - 1].term() != request.prev_log_term) {
            // The terms don't match
            return response;
        }
    }
    
    // Append the entries
    if (!request.entries.empty()) {
        // Find conflicting entries
        size_t conflict_index = 0;
        for (size_t i = 0; i < request.entries.size(); ++i) {
            uint64_t log_index = request.prev_log_index + i + 1;
            
            if (log_index > log_.size()) {
                // No more entries in our log, no conflict
                break;
            }
            
            if (log_[log_index - 1].term() != request.entries[i].term()) {
                // Found a conflict
                conflict_index = i;
                
                // Remove all entries from this point on
                log_.resize(log_index - 1);
                break;
            }
        }
        
        // Append new entries
        for (size_t i = conflict_index; i < request.entries.size(); ++i) {
            log_.push_back(request.entries[i]);
        }
        
        // Save state
        save_state();
    }
    
    // Update commit index
    if (request.leader_commit > commit_index_) {
        uint64_t new_commit_index = std::min(request.leader_commit, get_last_log_index());
        commit_index_.store(new_commit_index);
        
        // Notify the apply thread
        apply_cv_.notify_one();
    }
    
    // Set success and match index
    response.success = true;
    response.match_index = request.prev_log_index + request.entries.size();
    
    return response;
}

void RaftNode::send_heartbeats() {
    // Send heartbeats/AppendEntries to all peers
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        std::thread([this, i]() {
            replicate_log(i);
        }).detach();
    }
}

void RaftNode::replicate_log(size_t peer_index) {
    const std::string& peer = peer_addresses_[peer_index];
    
    try {
        // Create the request
        AppendEntriesRequest request;
        std::vector<raft::LogEntry> entries;
        uint64_t prev_log_index;
        uint64_t prev_log_term;
        
        {
            std::lock_guard<std::mutex> lock(mutex_);
            
            // If we're no longer the leader, don't send anything
            if (role_ != ServerRole::LEADER) {
                return;
            }
            
            request.leader_id = node_id_;
            request.term = current_term_;
            request.leader_commit = commit_index_;
            
            // Get the previous log index and term
            prev_log_index = next_index_[peer_index] - 1;
            prev_log_term = 0;
            
            if (prev_log_index > 0 && prev_log_index <= log_.size()) {
                prev_log_term = log_[prev_log_index - 1].term();
            }
            
            request.prev_log_index = prev_log_index;
            request.prev_log_term = prev_log_term;
            
            // Get the entries to send
            for (size_t i = next_index_[peer_index]; i <= log_.size(); ++i) {
                entries.push_back(log_[i - 1]);
            }
            
            request.entries = entries;
        }
        
        // Send the request
        AppendEntriesResponse response = rpc_->append_entries(peer, request);
        
        // Process the response
        std::lock_guard<std::mutex> lock(mutex_);
        
        // If we're no longer the leader, ignore the response
        if (role_ != ServerRole::LEADER) {
            return;
        }
        
        // If the response term is higher than ours, step down
        if (response.term > current_term_) {
            step_down(response.term);
            return;
        }
        
        // If the request was successful, update next_index and match_index
        if (response.success) {
            next_index_[peer_index] = response.match_index + 1;
            match_index_[peer_index] = response.match_index;
            
            // Check if we can advance the commit index
            std::vector<uint64_t> match_indices = match_index_;
            match_indices.push_back(log_.size()); // Include self
            
            std::sort(match_indices.begin(), match_indices.end());
            uint64_t majority_index = match_indices[match_indices.size() / 2];
            
            if (majority_index > commit_index_ && 
                (log_.empty() || log_[majority_index - 1].term() == current_term_)) {
                commit_index_.store(majority_index);
                
                // Notify the apply thread
                apply_cv_.notify_one();
                
                std::cout << "Leader " << node_id_ << " advanced commit index to " 
                          << commit_index_ << std::endl;
            }
        } else {
            // If the request failed, decrement next_index and try again
            if (next_index_[peer_index] > 1) {
                next_index_[peer_index]--;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error replicating log to " << peer << ": " << e.what() << std::endl;
    }
}

uint64_t RaftNode::get_last_log_index() const {
    return log_.empty() ? 0 : log_.back().index();
}

uint64_t RaftNode::get_last_log_term() const {
    return log_.empty() ? 0 : log_.back().term();
}

bool RaftNode::append_entries(uint64_t prev_log_index, uint64_t prev_log_term, 
                              const std::vector<raft::LogEntry>& entries) {
    // Check if the previous log entry matches
    if (prev_log_index > 0) {
        if (prev_log_index > log_.size()) {
            // We don't have the previous log entry
            return false;
        }
        
        if (log_[prev_log_index - 1].term() != prev_log_term) {
            // The terms don't match
            return false;
        }
    }
    
    // Append the entries
    if (!entries.empty()) {
        // Find conflicting entries
        size_t conflict_index = 0;
        for (size_t i = 0; i < entries.size(); ++i) {
            uint64_t log_index = prev_log_index + i + 1;
            
            if (log_index > log_.size()) {
                // No more entries in our log, no conflict
                break;
            }
            
            if (log_[log_index - 1].term() != entries[i].term()) {
                // Found a conflict
                conflict_index = i;
                
                // Remove all entries from this point on
                log_.resize(log_index - 1);
                break;
            }
        }
        
        // Append new entries
        for (size_t i = conflict_index; i < entries.size(); ++i) {
            log_.push_back(entries[i]);
        }
    }
    
    return true;
}

void RaftNode::save_state() {
    if (storage_dir_.empty()) {
        return;
    }
    
    try {
        // Ensure storage directory exists
        std::filesystem::create_directories(storage_dir_);

        // Save current term and voted_for
        std::string metadata_path = storage_dir_ + "/metadata.txt";
        std::ofstream metadata_file(metadata_path);
        metadata_file << current_term_ << " " << voted_for_ << std::endl;
        
        // Save log entries
        std::string log_path = storage_dir_ + "/log.txt";
        std::ofstream log_file(log_path);
        for (const auto& entry : log_) {
            log_file << entry.serialize() << std::endl;
        }

        // Save state machine snapshot
        std::string snapshot_data = state_machine_->create_snapshot();
        uint64_t last_included_index = last_applied_; 
        // Optional: Get term for compaction later
        // uint64_t last_included_term = (last_included_index > 0 && last_included_index <= log_.size()) ? log_[last_included_index - 1].term() : 0;

        std::string snapshot_path = storage_dir_ + "/snapshot.dat";
        std::ofstream snapshot_file(snapshot_path);
        // Store last applied index first, then the snapshot data
        snapshot_file << last_included_index << "\n"; 
        snapshot_file << snapshot_data;

    } catch (const std::exception& e) {
        std::cerr << "Error saving state: " << e.what() << std::endl;
    }
}

void RaftNode::load_state() {
    if (storage_dir_.empty()) {
        return;
    }
    
    try {
        // Load current term and voted_for
        std::string metadata_path = storage_dir_ + "/metadata.txt";
        if (std::filesystem::exists(metadata_path)) {
            std::ifstream metadata_file(metadata_path);
            uint64_t term;
            metadata_file >> term >> voted_for_;
            current_term_ = term;
        }
        
        // Load log entries
        std::string log_path = storage_dir_ + "/log.txt";
        if (std::filesystem::exists(log_path)) {
            std::ifstream log_file(log_path);
            std::string line;
            while (std::getline(log_file, line)) {
                if (!line.empty()) {
                    log_.push_back(raft::LogEntry::deserialize(line));
                }
            }
        }

        // Load state machine snapshot
        std::string snapshot_path = storage_dir_ + "/snapshot.dat";
        if (std::filesystem::exists(snapshot_path)) {
            std::ifstream snapshot_file(snapshot_path);
            std::string line;
            
            // Read last applied index
            if (std::getline(snapshot_file, line)) {
                 try {
                    last_applied_ = std::stoull(line);
                    // Read the rest of the file as snapshot data
                    std::stringstream ss;
                    ss << snapshot_file.rdbuf();
                    std::string snapshot_data = ss.str();

                    // Restore state machine
                    if (!state_machine_->restore_snapshot(snapshot_data)) {
                         std::cerr << "Node " << node_id_ << " failed to restore snapshot." << std::endl;
                         // If restore fails, might need to clear state or handle differently
                         last_applied_ = 0; // Reset last_applied if snapshot fails
                    } else {
                         std::cout << "Node " << node_id_ << " restored snapshot up to index " << last_applied_ << std::endl;
                         // Adjust commit_index if necessary (Raft guarantees commit_index >= last_applied)
                         if (commit_index_ < last_applied_) {
                            commit_index_.store(last_applied_);
                         }
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing last_applied index from snapshot file: " << e.what() << std::endl;
                    last_applied_ = 0; // Reset on error
                }
            } else {
                 std::cerr << "Snapshot file " << snapshot_path << " is empty or corrupted." << std::endl;
                 last_applied_ = 0; // Reset if file format is wrong
            }
        } else {
             // No snapshot found, last_applied remains 0
             std::cout << "Node " << node_id_ << " found no snapshot file, starting from index 0." << std::endl;
        }
        
        std::cout << "Node " << node_id_ << " loaded state: term=" << current_term_ 
                  << ", voted_for=" << voted_for_ << ", log_size=" << log_.size() 
                  << ", last_applied=" << last_applied_ << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error loading state: " << e.what() << std::endl;
    }
}

void RaftNode::reset_election_timeout() {
    last_heartbeat_ = std::chrono::steady_clock::now();
    election_timeout_ = random_election_timeout();
}

std::chrono::milliseconds RaftNode::random_election_timeout() {
    // Generate a random timeout between 150ms and 300ms
    std::uniform_int_distribution<> dist(150, 300);
    return std::chrono::milliseconds(dist(random_engine_));
}
