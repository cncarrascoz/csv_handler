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
    current_term_++;
    role_ = ServerRole::CANDIDATE;
    voted_for_ = node_id_; // Vote for self
    current_leader_ = "";
    
    std::cout << "Node " << node_id_ << " is now a candidate (term " << current_term_ << ")" << std::endl;
    
    // In a real implementation:
    // 1. Send RequestVote RPCs to all peers
    // 2. Wait for responses
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
    // For each peer, send AppendEntriesArgs (empty or with entries)
    // (Stub: actual transport not implemented)
}

void RaftNode::handle_append_entries_reply(size_t peer_idx, const AppendEntriesArgs& sent, const AppendEntriesReply& reply) {
    // Update next_index_, match_index_, commit_index_ per Raft
    // (Stub: actual logic not implemented)
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
