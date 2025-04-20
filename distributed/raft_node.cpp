#include "raft_node.hpp"
#include <iostream>
#include <random>

RaftNode::RaftNode(
    const std::string& node_id,
    std::shared_ptr<IStateMachine> state_machine,
    const std::vector<std::string>& peer_addresses)
    : node_id_(node_id),
      state_machine_(state_machine),
      peer_addresses_(peer_addresses) {
    
    // Initialize with empty indices for each peer
    next_index_.resize(peer_addresses_.size(), 1);
    match_index_.resize(peer_addresses_.size(), 0);
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
    
    // Placeholder for log append and replication
    std::cout << "Leader " << node_id_ << " received mutation (replication not implemented)" << std::endl;
    
    // Apply locally for now (this would normally happen after consensus)
    state_machine_->apply(mutation);
    
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
    std::random_device rd;
    std::mt19937 gen(rd());
    
    // Simulate the Raft ticker loop
    while (running_) {
        auto role = role_.load();
        
        switch (role) {
            case ServerRole::FOLLOWER: {
                // In a real implementation:
                // 1. Check if election timeout has elapsed
                // 2. If so, become candidate
                break;
            }
            case ServerRole::CANDIDATE: {
                // In a real implementation:
                // 1. Start election
                // 2. Request votes from peers
                break;
            }
            case ServerRole::LEADER: {
                // In a real implementation:
                // 1. Send heartbeats to followers
                // 2. Check replication status
                send_heartbeats();
                break;
            }
            case ServerRole::STANDALONE:
                // Nothing to do
                break;
        }
        
        // Sleep for a short time
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
        next_index_[i] = commit_index_ + 1;
        match_index_[i] = 0;
    }
    
    std::cout << "Node " << node_id_ << " is now the leader (term " << current_term_ << ")" << std::endl;
    
    // Send initial heartbeats
    send_heartbeats();
}

void RaftNode::send_heartbeats() {
    // In a real implementation:
    // 1. Create AppendEntries RPCs with no entries (heartbeats)
    // 2. Send to all peers
    std::cout << "Leader " << node_id_ << " sending heartbeats (stub implementation)" << std::endl;
}

void RaftNode::handle_heartbeat(const Heartbeat& heartbeat) {
    // Process incoming heartbeat
    uint64_t term = heartbeat.term;
    
    // If the heartbeat's term is greater than ours, step down
    if (term > current_term_) {
        step_down(term);
    }
    
    // If we're a follower and this is from the current leader, reset election timeout
    if (role_ == ServerRole::FOLLOWER && heartbeat.is_leader && term == current_term_) {
        current_leader_ = heartbeat.node_id;
        // Reset election timeout
    }
}
