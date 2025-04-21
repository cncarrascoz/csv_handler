#pragma once

#include "core/IStateMachine.hpp"
#include "core/Mutation.hpp"
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <cstdint>
#include <atomic>
#include <thread>

/**
 * Enum defining the possible roles a node can have in the cluster
 */
enum class ServerRole {
    STANDALONE, // Default role, not part of a cluster
    LEADER,     // Leader node, processes writes and replicates to followers
    FOLLOWER,   // Follower node, receives updates from leader
    CANDIDATE   // Node campaigning to become leader
};

/**
 * RaftNode implements the Raft consensus algorithm for distributed consensus
 */
class RaftNode {
public:
    /**
     * Constructor
     * @param node_id Unique identifier for this node
     * @param state_machine State machine to apply operations to
     * @param peer_addresses Addresses of other nodes in the cluster
     */
    RaftNode(const std::string& node_id,
             std::shared_ptr<IStateMachine> state_machine,
             const std::vector<std::string>& peer_addresses);
    
    /**
     * Destructor - cleans up resources
     */
    ~RaftNode();
    
    /**
     * Start the Raft node
     */
    void start();
    
    /**
     * Stop the Raft node
     */
    void stop();
    
    /**
     * Submit a mutation to the cluster
     * @param mutation The mutation to apply
     * @return True if the mutation was accepted (doesn't guarantee it will be committed)
     */
    bool submit(const Mutation& mutation);
    
    /**
     * Get the current role of this node
     * @return The current server role
     */
    ServerRole role() const;
    
    /**
     * Get the current term
     * @return The current term
     */
    uint64_t current_term() const;
    
    /**
     * Get the ID of the current leader
     * @return The leader's ID, or empty string if unknown
     */
    std::string current_leader() const;
    
    // In a real implementation, this would be a protobuf message
    // For now, we'll use a simplified structure
    struct Heartbeat {
        std::string node_id;
        uint64_t term;
        bool is_leader;
        uint64_t last_committed_index;
    };

private:
    std::string node_id_;
    std::shared_ptr<IStateMachine> state_machine_;
    std::vector<std::string> peer_addresses_;
    
    // Raft state
    std::atomic<ServerRole> role_{ServerRole::STANDALONE};
    std::atomic<uint64_t> current_term_{0};
    std::string voted_for_;
    std::string current_leader_;
    uint64_t commit_index_{0};
    uint64_t last_applied_{0};
    
    // Volatile leader state
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;
    
    // Control flags
    std::atomic<bool> running_{false};
    std::thread ticker_thread_;
    
    // Ticker function for periodic tasks
    void tick();
    
    // Core Raft steps
    void step_down(uint64_t term);
    void become_follower(uint64_t term, const std::string& leader_id);
    void become_candidate();
    void become_leader();
    
    // Handle heartbeats
    void send_heartbeats();
    
    void handle_heartbeat(const Heartbeat& heartbeat);
};
