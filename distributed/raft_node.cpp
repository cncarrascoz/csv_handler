#include "raft_node.hpp"
#include <iostream>
#include <random>
#include <mutex>

RaftNode::RaftNode(
    const std::string& node_id,
    std::shared_ptr<IStateMachine> state_machine,
    const std::vector<std::string>& peer_addresses)
    : node_id_(node_id),
      state_machine_(state_machine),
      peer_addresses_(peer_addresses),
      log_({{0,{}}}),
      rand_gen_(std::random_device{}()),
      role_(ServerRole::STANDALONE),
      current_term_(0),
      commit_index_(0),
      last_applied_(0),
      running_(false) {
    
    // Initialize with empty indices for each peer
    next_index_.resize(peer_addresses_.size(), 1);
    match_index_.resize(peer_addresses_.size(), 0);
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(150);
    heartbeat_deadline_ = std::chrono::steady_clock::now();
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }
    
    // Start in follower state if we have peers, otherwise standalone
    if (peer_addresses_.empty()) {
        role_ = ServerRole::STANDALONE;
        std::cout << "Node " << node_id_ << " starting in STANDALONE mode" << std::endl;
    } else {
        role_ = ServerRole::FOLLOWER;
        std::cout << "Node " << node_id_ << " starting in FOLLOWER mode" << std::endl;
        
        // Start the ticker thread for heartbeats, elections, etc.
        ticker_thread_ = std::thread(&RaftNode::tick, this);
    }
}

void RaftNode::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    // Wait for the ticker thread to finish
    if (ticker_thread_.joinable()) {
        ticker_thread_.join();
    }
    
    std::cout << "Node " << node_id_ << " stopped" << std::endl;
}

bool RaftNode::submit(const Mutation& mutation) {
    // In STANDALONE mode, apply directly
    if (role_ == ServerRole::STANDALONE) {
        state_machine_->apply(mutation);
        return true;
    }
    
    // In a real implementation:
    // 1. If not leader, forward to leader or reject
    // 2. If leader, append to local log and replicate to followers
    
    if (role_ != ServerRole::LEADER) {
        std::cout << "Cannot submit mutation: not the leader" << std::endl;
        return false;
    }
    
    // Append to log and replicate
    log_.push_back({current_term_, mutation});
    std::cout << "Leader " << node_id_ << " received mutation (appended to log, will replicate)" << std::endl;
    send_append_entries_to_all(false); // Replicate new entry
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

void RaftNode::tick() {
    // Simulate the Raft ticker loop
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // 10ms granularity
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mu_);
        switch (role_) {
            case ServerRole::FOLLOWER:
            case ServerRole::CANDIDATE:
                if (now >= election_deadline_) {
                    become_candidate();
                    reset_election_deadline(rand_gen_);
                }
                break;
            case ServerRole::LEADER:
                if (now >= heartbeat_deadline_) {
                    send_append_entries_to_all(true);
                    heartbeat_deadline_ = now + std::chrono::milliseconds(50);
                }
                break;
            case ServerRole::STANDALONE:
                break;
        }
    }
}

void RaftNode::step_down(uint64_t term) {
    if (term > current_term_) {
        current_term_ = term;
        voted_for_ = "";
        become_follower(term, "");
    }
}

void RaftNode::become_follower(uint64_t term, const std::string& leader_id) {
    role_ = ServerRole::FOLLOWER;
    current_leader_ = leader_id;
    std::cout << "Node " << node_id_ << " is now a follower (term " << term << ")" << std::endl;
    
    // Reset election timeout
    reset_election_deadline(rand_gen_);
}

void RaftNode::become_candidate() {
    std::cout << "Node " << node_id_ << " is now a candidate (term " << current_term_ + 1 << ")" << std::endl;
    
    // Increment term and vote for self
    ++current_term_;
    role_ = ServerRole::CANDIDATE;
    voted_for_ = node_id_;
    current_leader_ = "";
    
    // Count votes (start with self-vote)
    size_t votes = 1;
    size_t votes_needed = (peer_addresses_.size() + 1) / 2 + 1;
    
    // Reset election timer
    reset_election_deadline(rand_gen_);
    
    // Prepare RequestVote args
    RequestVoteArgs args;
    args.term = current_term_;
    args.candidate_id = node_id_;
    args.last_log_index = log_.size() - 1;
    args.last_log_term = log_.back().term;
    
    // Send RequestVote RPCs to all peers
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        // This would be asynchronous in a real implementation
        // For now, simulate a synchronous call for teaching purposes
        
        // In a real implementation, this would be a gRPC call:
        // auto reply = stub_[i]->RequestVote(args);
        
        RequestVoteReply reply;
        
        // For now, simulate the RPC with a direct call to the target node
        // This can be replaced with actual RPC calls when connected to gRPC
        
        // Simulated failure for teaching purposes (20% chance)
        if (rand_gen_() % 5 == 0) {
            std::cout << "RequestVote RPC to " << peer_addresses_[i] << " failed (simulated)" << std::endl;
            continue;
        }
        
        // Simulate success response with 50% chance of getting vote
        reply.term = current_term_;
        reply.vote_granted = (rand_gen_() % 2 == 0);
        
        if (reply.term > current_term_) {
            // Discovered higher term, revert to follower
            step_down(reply.term);
            return;
        }
        
        if (reply.vote_granted) {
            ++votes;
            std::cout << "Node " << node_id_ << " received vote from " << peer_addresses_[i] 
                      << " (" << votes << "/" << votes_needed << ")" << std::endl;
            
            if (votes >= votes_needed) {
                become_leader();
                return;
            }
        }
    }
    
    // If we didn't get enough votes, remain as candidate for now
    // The election timeout will trigger another election if needed
}

void RaftNode::become_leader() {
    if (role_ != ServerRole::CANDIDATE) {
        return;
    }
    role_ = ServerRole::LEADER;
    current_leader_ = node_id_;
    // Initialize leader state
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        next_index_[i] = log_.size(); // Next log index to send
        match_index_[i] = 0;
    }
    std::cout << "Node " << node_id_ << " is now the leader (term " << current_term_ << ")" << std::endl;
    heartbeat_deadline_ = std::chrono::steady_clock::now();
    send_append_entries_to_all(true); // Immediate empty AppendEntries (heartbeat)
}

void RaftNode::send_append_entries_to_all(bool empty_only) {
    // Only leaders can send AppendEntries
    if (role_ != ServerRole::LEADER) {
        return;
    }
    
    std::cout << "Leader " << node_id_ << " sending AppendEntries to all followers (empty_only=" 
              << (empty_only ? "true" : "false") << ")" << std::endl;
    
    // For each peer, prepare and send AppendEntries RPC
    for (size_t i = 0; i < peer_addresses_.size(); ++i) {
        // Prepare basic AppendEntries request with common fields
        AppendEntriesArgs args;
        args.term = current_term_;
        args.leader_id = node_id_;
        args.leader_commit = commit_index_;
        
        // Include log entries if this isn't just a heartbeat
        if (!empty_only) {
            // Find entries to send: all entries from next_index_[i] to end of log
            uint64_t prev_log_idx = next_index_[i] - 1;
            args.prev_log_index = prev_log_idx;
            args.prev_log_term = (prev_log_idx > 0 && prev_log_idx < log_.size()) ? 
                                  log_[prev_log_idx].term : 0;
            
            // Include entries starting from next_index_[i]
            for (size_t j = next_index_[i]; j < log_.size(); ++j) {
                args.entries.push_back(log_[j]);
            }
        } else {
            // For heartbeat, just send previous log info
            uint64_t prev_log_idx = log_.size() - 1;
            args.prev_log_index = prev_log_idx;
            args.prev_log_term = log_[prev_log_idx].term;
            // Leave entries empty for heartbeat
        }
        
        // In a real implementation, this would be an asynchronous gRPC call:
        // auto reply_future = stub_[i]->AppendEntries(args);
        // reply_future.then([this, i, args](AppendEntriesReply reply) {
        //     handle_append_entries_reply(i, args, reply);
        // });
        
        // For demo purposes, simulate the RPC with random success/failure
        if (rand_gen_() % 10 < 2) { // 20% chance of "network failure"
            std::cout << "AppendEntries to " << peer_addresses_[i] << " failed (simulated)" << std::endl;
            continue;
        }
        
        // Simulate a successful response
        AppendEntriesReply reply;
        reply.term = current_term_;
        reply.success = true;
        
        // Process the response
        handle_append_entries_reply(i, args, reply);
    }
}

void RaftNode::handle_append_entries_reply(size_t peer_idx, const AppendEntriesArgs& sent, const AppendEntriesReply& reply) {
    std::lock_guard<std::mutex> lock(mu_);
    
    // If no longer leader or term has changed, ignore
    if (role_ != ServerRole::LEADER || current_term_ != sent.term) {
        return;
    }
    
    // If follower reported higher term, step down
    if (reply.term > current_term_) {
        step_down(reply.term);
        return;
    }
    
    if (reply.success) {
        // Success: Update tracking for this follower
        
        // Update match_index and next_index for the follower
        if (!sent.entries.empty()) {
            uint64_t last_entry_idx = sent.prev_log_index + sent.entries.size();
            match_index_[peer_idx] = last_entry_idx;
            next_index_[peer_idx] = last_entry_idx + 1;
            
            std::cout << "Leader: Follower " << peer_addresses_[peer_idx] 
                      << " successfully replicated up to index " << last_entry_idx << std::endl;
            
            // Check if we can advance the commit index
            update_commit_index();
        }
    } else {
        // Failed due to log inconsistency
        if (reply.conflict_term > 0) {
            // Follower has conflicting entry at conflict_index with term conflict_term
            // Find the last index with this term in our log
            uint64_t last_idx_for_term = 0;
            for (size_t i = 1; i < log_.size(); ++i) {
                if (log_[i].term == reply.conflict_term) {
                    last_idx_for_term = i;
                }
            }
            
            if (last_idx_for_term > 0) {
                // We have entries from this term, backtrack to the last one
                next_index_[peer_idx] = last_idx_for_term + 1;
            } else {
                // We don't have entries from this term, use follower's first index
                next_index_[peer_idx] = reply.conflict_index;
            }
        } else {
            // Follower doesn't have prev_log_index entry
            // Set next_index to follower's last log index + 1
            next_index_[peer_idx] = reply.conflict_index;
        }
        
        std::cout << "Leader: AppendEntries to " << peer_addresses_[peer_idx] 
                  << " failed, backtracking to index " << next_index_[peer_idx] << std::endl;
    }
}

void RaftNode::update_commit_index() {
    // Only leaders update commit index based on replication status
    if (role_ != ServerRole::LEADER) {
        return;
    }
    
    // Make a copy of match indices to find the median
    std::vector<uint64_t> indices = match_index_;
    indices.push_back(log_.size() - 1); // Include leader's log
    
    // Sort to find the majority-replicated index (median)
    std::sort(indices.begin(), indices.end());
    uint64_t majority_idx = indices[indices.size() / 2];
    
    // Can only commit logs from current term (Raft safety property)
    // and only if it's greater than current commit_index_
    if (majority_idx > commit_index_ && log_[majority_idx].term == current_term_) {
        uint64_t old_commit_idx = commit_index_;
        commit_index_ = majority_idx;
        
        std::cout << "Leader: Advanced commit index from " << old_commit_idx 
                  << " to " << commit_index_ << std::endl;
        
        // Apply newly committed entries to state machine
        apply_entries();
        
        // Notify followers of the new commit index with a heartbeat
        send_append_entries_to_all(true);
    }
}

RaftNode::AppendEntriesReply RaftNode::handle_append_entries(const AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lock(mu_);
    AppendEntriesReply reply{current_term_, false, 0, 0};
    // 1. Reply false if term < current_term_
    if (args.term < current_term_) {
        reply.term = current_term_;
        reply.success = false;
        return reply;
    }
    // 2. If term > current_term_, step down
    if (args.term > current_term_) {
        step_down(args.term);
        current_leader_ = args.leader_id;
    }
    // 3. Reset election timeout
    reset_election_deadline(rand_gen_);
    // 4. Consistency check: log length and term match
    if (args.prev_log_index >= log_.size() ||
        log_[args.prev_log_index].term != args.prev_log_term) {
        // Conflict: reply with info to help leader
        reply.term = current_term_;
        reply.success = false;
        if (args.prev_log_index >= log_.size()) {
            reply.conflict_index = log_.size();
            reply.conflict_term = 0;
        } else {
            reply.conflict_term = log_[args.prev_log_index].term;
            size_t i = args.prev_log_index;
            while (i > 0 && log_[i-1].term == reply.conflict_term) --i;
            reply.conflict_index = i;
        }
        return reply;
    }
    // 5. If existing entries conflict with new ones, delete all that follow
    size_t idx = args.prev_log_index + 1;
    size_t entry_idx = 0;
    while (idx < log_.size() && entry_idx < args.entries.size()) {
        if (log_[idx].term != args.entries[entry_idx].term) {
            log_.resize(idx);
            break;
        }
        ++idx; ++entry_idx;
    }
    // 6. Append any new entries not already in the log
    while (entry_idx < args.entries.size()) {
        log_.push_back(args.entries[entry_idx++]);
    }
    // 7. If leader_commit > commit_index_, set commit_index_ = min(leader_commit, last new entry)
    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, static_cast<uint64_t>(log_.size()-1));
        apply_entries();
    }
    reply.term = current_term_;
    reply.success = true;
    return reply;
}

void RaftNode::apply_entries() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        if (last_applied_ < log_.size()) {
            state_machine_->apply(log_[last_applied_].cmd);
        }
    }
}

// Helper: reset_election_deadline (random 150-300ms)
void RaftNode::reset_election_deadline(std::mt19937& gen) {
    std::uniform_int_distribution<int> dist(150, 300);
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(gen));
}

RaftNode::RequestVoteReply RaftNode::handle_request_vote(const RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lock(mu_);
    RequestVoteReply reply;
    reply.term = current_term_;
    reply.vote_granted = false;

    if (args.term < current_term_) {
        return reply;
    }
    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = "";
        role_ = ServerRole::FOLLOWER;
        // persist state if needed
    }
    bool up_to_date = 
        (args.last_log_term > log_.back().term) ||
        (args.last_log_term == log_.back().term && args.last_log_index >= log_.size() - 1);
    if ((voted_for_ == "" || voted_for_ == args.candidate_id) && up_to_date) {
        voted_for_ = args.candidate_id;
        reply.vote_granted = true;
        // reset election timer
        reset_election_deadline(rand_gen_);
    }
    return reply;
}
