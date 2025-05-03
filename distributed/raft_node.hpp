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
#include <mutex>
#include <random>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include "proto/csv_service.grpc.pb.h"

// Forward declaration
namespace network {
    class ServerRegistry;
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
 * 
 * This implementation follows the core Raft protocol as described in:
 * https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/
 * 
 * Key components:
 * - Leader election: Followers become candidates after timeout, request votes
 * - Log replication: Leader appends entries to log, replicates to followers
 * - Safety: Ensures all nodes apply the same commands in the same order
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
    
    /**
     * Log entry structure - represents a single entry in the Raft log
     * Each entry contains:
     * - term: The term when the entry was created
     * - cmd: The mutation to apply to the state machine
     */
    struct LogEntry {
        uint64_t term;
        Mutation cmd;
    };

    /**
     * AppendEntries RPC arguments
     * Sent by leader to replicate log entries and as heartbeats
     */
    struct AppendEntriesArgs {
        uint64_t term;                  // Leader's term
        std::string leader_id;          // Leader's ID so followers can redirect clients
        uint64_t prev_log_index;        // Index of log entry immediately preceding new ones
        uint64_t prev_log_term;         // Term of prev_log_index entry
        std::vector<LogEntry> entries;  // Log entries to store (empty for heartbeat)
        uint64_t leader_commit;         // Leader's commit index
    };

    /**
     * AppendEntries RPC response
     * Returned by followers to indicate success or provide info for faster retries
     */
    struct AppendEntriesReply {
        uint64_t term;              // Current term, for leader to update itself
        bool success;               // True if follower contained entry matching prev_log_index/term
        uint64_t conflict_index;    // Index of conflicting entry (for faster retries)
        uint64_t conflict_term;     // Term of conflicting entry
    };

    /**
     * RequestVote RPC arguments
     * Sent by candidates to gather votes
     */
    struct RequestVoteArgs {
        uint64_t term;              // Candidate's term
        std::string candidate_id;   // Candidate requesting vote
        uint64_t last_log_index;    // Index of candidate's last log entry
        uint64_t last_log_term;     // Term of candidate's last log entry
    };

    /**
     * RequestVote RPC response
     * Returned by followers to indicate if vote was granted
     */
    struct RequestVoteReply {
        uint64_t term;              // Current term, for candidate to update itself
        bool vote_granted;          // True if candidate received vote
    };

    /**
     * Heartbeat message structure (for debugging/monitoring)
     */
    struct Heartbeat {
        std::string node_id;
        uint64_t term;
        bool is_leader;
        uint64_t last_committed_index;
    };

    // Public RPC handlers (exposed for testing and direct in-process calls)
    void handle_append_entries(const AppendEntriesArgs& args, AppendEntriesReply& reply);
    /**
     * This method is called by the gRPC service implementation when it receives an AppendEntries RPC
     * 
     * @param request The gRPC request
     * @param response The gRPC response to send back
     */
    void handle_append_entries_rpc(const csvservice::ReplicateMutationRequest& request,
                                 csvservice::ReplicateMutationResponse& response);
    void handle_request_vote(const RequestVoteArgs& args, RequestVoteReply& reply);
    void handle_request_vote_rpc(const csvservice::RequestVoteRequest& request,
                               csvservice::RequestVoteResponse& response);

    // Set the server registry for leader updates
    void set_server_registry(network::ServerRegistry* registry);

    /**
     * Thread-safe wrapper for handle_append_entries
     * 
     * @param args The arguments of the RPC
     * @param reply The reply to send back
     */
    void handle_append_entries_safe(const AppendEntriesArgs& args, AppendEntriesReply& reply) {
        std::lock_guard<std::mutex> lock(mu_);
        handle_append_entries(args, reply);
    }
    
    /**
     * Thread-safe wrapper for handle_request_vote
     * 
     * @param args The arguments of the RPC
     * @param reply The reply to send back
     */
    void handle_request_vote_safe(const RequestVoteArgs& args, RequestVoteReply& reply) {
        std::lock_guard<std::mutex> lock(mu_);
        handle_request_vote(args, reply);
    }

private:
    // Basic node identification and state
    std::string node_id_;                              // Unique identifier for this node
    std::shared_ptr<IStateMachine> state_machine_;     // State machine to apply operations to
    std::vector<std::string> peer_addresses_;          // Addresses of other nodes in the cluster
    std::vector<std::unique_ptr<csvservice::CsvService::Stub>> peer_stubs_; // gRPC stubs for peers
    
    // Persistent Raft state (should be persisted before responding to RPCs)
    std::atomic<uint64_t> current_term_;               // Latest term server has seen
    std::string voted_for_;                            // CandidateId that received vote in current term
    std::vector<LogEntry> log_;                        // Log entries (first index is 1)
    
    // Volatile Raft state on all servers
    std::atomic<ServerRole> role_;                     // Current role of this node
    std::atomic<uint64_t> commit_index_;               // Highest log entry known to be committed
    std::atomic<uint64_t> last_applied_;               // Highest log entry applied to state machine
    std::string current_leader_;                       // Current leader's ID (for client redirection)
    std::vector<bool> votes_received_;                 // Tracks votes received during an election
    
    // Volatile Raft state on leaders (reinitialized after election)
    std::vector<uint64_t> next_index_;                 // Index of next log entry to send to each server
    std::vector<uint64_t> match_index_;                // Index of highest log entry known to be replicated
    
    // Server registry for leader updates
    network::ServerRegistry* server_registry_ = nullptr;

    // Timers and randomization
    std::chrono::steady_clock::time_point election_deadline_;    // When to start an election
    std::chrono::steady_clock::time_point heartbeat_deadline_;   // When to send next heartbeat
    std::chrono::steady_clock::time_point last_heartbeat_;       // Last time we received a heartbeat
    std::chrono::milliseconds election_timeout_;                 // Randomized election timeout
    std::chrono::milliseconds heartbeat_interval_ = std::chrono::milliseconds(100); // Heartbeat interval
    std::mt19937 rng_;                                           // Random number generator
    
    // Thread management
    std::atomic<bool> running_;                        // Flag to control background threads
    std::thread ticker_thread_;                        // Thread for periodic tasks
    std::mutex mu_;                                    // Mutex for thread safety
    std::condition_variable apply_cv_;                 // Condition variable for apply thread
    std::thread apply_thread_;                         // Thread for applying committed entries
    
    // Core Raft methods
    void tick();                                       // Main ticker function for periodic tasks
    void apply_entries_loop();                         // Loop to apply committed entries
    
    // Role transition methods
    void step_down(uint64_t term);                     // Step down to follower when higher term seen
    void become_follower(uint64_t term, const std::string& leader_id); // Transition to follower
    void become_candidate();                           // Transition to candidate and start election
    void become_leader();                              // Transition to leader after winning election
    
    // RPC methods
    void send_append_entries_to_all(bool heartbeat_only = false); // Send AppendEntries to all peers
    void send_append_entries(size_t peer_idx);            // Send AppendEntries to specific peer
    bool send_append_entries_rpc(size_t peer_idx, const AppendEntriesArgs& args, AppendEntriesReply& reply); // Send the actual RPC
    void handle_append_entries_reply(size_t peer_idx, const AppendEntriesArgs& args, const AppendEntriesReply& reply);
    void request_votes_from_all();                     // Send RequestVote to all peers
    
    // Helper methods for log management
    void update_commit_index();                         // Update commit index based on match indices
    
    // Helper methods
    void reset_election_deadline();                    // Reset election timeout with random value
    void apply_entries();                              // Apply committed entries to state machine
    bool is_log_up_to_date(uint64_t last_log_index, uint64_t last_log_term); // Check if log is up to date
    uint64_t get_last_log_index() const;               // Get index of last log entry
    uint64_t get_last_log_term() const;                // Get term of last log entry
    void create_peer_stubs();                          // Create gRPC stubs for peer communication
    void handle_request_vote_reply(int peer_idx, const RequestVoteArgs& args, const RequestVoteReply& reply);
};
