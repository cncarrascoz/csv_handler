#pragma once

#include "core/RaftStateMachine.hpp"
#include "core/Mutation.hpp"
#include "core/LogEntry.hpp"
#include "rpc/IRaftRPC.hpp"

#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <cstdint>
#include <atomic>
#include <thread>
#include <mutex>
#include <random>
#include <unordered_map>
#include <condition_variable>
#include <fstream>

// Forward declarations
namespace raft {
class RaftStateMachine;
class Mutation;
class LogEntry;
enum class MutationType;
}

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
     * @param storage_dir Directory to store persistent state
     */
    RaftNode(const std::string& node_id,
             std::shared_ptr<raft::RaftStateMachine> state_machine,
             const std::vector<std::string>& peer_addresses,
             const std::string& storage_dir = "");
    
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
    bool submit(const raft::Mutation& mutation);
    
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
    
    /**
     * Get the number of nodes in the cluster
     * @return The cluster size
     */
    size_t cluster_size() const;
    
    /**
     * Get the number of nodes needed for a quorum
     * @return The quorum size
     */
    size_t quorum_size() const;
    
    /**
     * Get the addresses of all peers in the cluster
     * @return Vector of peer addresses
     */
    std::vector<std::string> peer_addresses() const;
    
    /**
     * Get the log entries
     * @return Vector of log entries
     */
    std::vector<raft::LogEntry> log_entries() const;
    
    /**
     * Get the commit index
     * @return The commit index
     */
    uint64_t commit_index() const;
    
    /**
     * Get the last applied index
     * @return The last applied index
     */
    uint64_t last_applied() const;
    
    /**
     * Get the state machine
     * @return The state machine
     */
    std::shared_ptr<raft::RaftStateMachine> state_machine() const {
        return state_machine_;
    }

private:
    std::string node_id_;
    std::shared_ptr<raft::RaftStateMachine> state_machine_;
    std::vector<std::string> peer_addresses_;
    std::string storage_dir_;
    
    // RPC client for communication with other nodes
    std::unique_ptr<IRaftRPC> rpc_;
    
    // Raft state
    std::atomic<ServerRole> role_{ServerRole::STANDALONE};
    std::atomic<uint64_t> current_term_{0};
    std::string voted_for_;
    std::string current_leader_;
    std::vector<raft::LogEntry> log_;
    std::atomic<uint64_t> commit_index_{0};
    std::atomic<uint64_t> last_applied_{0};
    
    // Volatile leader state
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;
    
    // Control flags
    std::atomic<bool> running_{false};
    std::thread ticker_thread_;
    std::thread apply_thread_;
    
    // Election timeout
    std::chrono::steady_clock::time_point last_heartbeat_;
    std::chrono::milliseconds election_timeout_;
    std::mt19937 random_engine_;
    
    // Mutex for protecting state
    mutable std::mutex mutex_;
    std::condition_variable apply_cv_;
    
    // Ticker function for periodic tasks
    void tick();
    
    // Apply committed entries to the state machine
    void apply_entries();
    
    // Core Raft steps
    void step_down(uint64_t term);
    void become_follower(uint64_t term, const std::string& leader_id);
    void become_candidate();
    void become_leader();
    
    // Handle RPC requests
    RequestVoteResponse handle_request_vote(const RequestVoteRequest& request);
    AppendEntriesResponse handle_append_entries(const AppendEntriesRequest& request);
    
    // Leader functions
    void send_heartbeats();
    void replicate_log(size_t peer_index);
    
    // Log manipulation
    uint64_t get_last_log_index() const;
    uint64_t get_last_log_term() const;
    bool append_entries(uint64_t prev_log_index, uint64_t prev_log_term, 
                        const std::vector<raft::LogEntry>& entries);
    
    // Persistence
    void save_state();
    void load_state();
    
    // Helper methods
    void reset_election_timeout();
    std::chrono::milliseconds random_election_timeout();
};
