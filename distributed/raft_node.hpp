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
    struct LogEntry {
        uint64_t term;
        Mutation cmd;
    };

    struct AppendEntriesArgs {
        uint64_t term;
        std::string leader_id;
        uint64_t prev_log_index;
        uint64_t prev_log_term;
        std::vector<LogEntry> entries;   // empty ⇒ heartbeat
        uint64_t leader_commit;
    };

    struct AppendEntriesReply {
        uint64_t term;
        bool     success;
        uint64_t conflict_index;         // leave 0 if unused
        uint64_t conflict_term;
    };

    struct Heartbeat {
        std::string node_id;
        uint64_t term;
        bool is_leader;
        uint64_t last_committed_index;
    };

    // Expose AppendEntries for in-process harness only
    AppendEntriesReply handle_append_entries(const AppendEntriesArgs& args);

    struct RequestVoteArgs {
        uint64_t term;
        std::string candidate_id;
        uint64_t last_log_index;
        uint64_t last_log_term;
    };
    
    struct RequestVoteReply {
        uint64_t term;
        bool vote_granted;
    };

    // Expose RequestVote for in-process harness only
    RequestVoteReply handle_request_vote(const RequestVoteArgs& args);

private:
    std::string node_id_;
    std::shared_ptr<IStateMachine> state_machine_;
    std::vector<std::string> peer_addresses_;
    
    // Raft state
    std::atomic<ServerRole> role_;
    std::atomic<uint64_t> current_term_;
    std::string voted_for_ = "";
    std::string current_leader_;
    uint64_t commit_index_;
    uint64_t last_applied_;
    
    // Persistent Raft log and timers
    std::vector<LogEntry> log_;
    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point heartbeat_deadline_;
    std::mt19937 rand_gen_;
    std::mutex mu_;
    
    // Volatile leader state
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;
    
    // Control flags
    std::atomic<bool> running_;
    std::thread ticker_thread_;
    
    // Ticker function for periodic tasks
    void tick();
    
    // Core Raft steps
    void step_down(uint64_t term);
    void become_follower(uint64_t term, const std::string& leader_id);
    void become_candidate();
    void become_leader();

    // AppendEntries RPCs
    void send_append_entries_to_all(bool empty_only);
    void handle_append_entries_reply(size_t peer_idx, const AppendEntriesArgs& sent, const AppendEntriesReply& reply);
    
    // Commit and apply helpers
    void update_commit_index();
    void apply_entries();
    
    // Helpers
    void reset_election_deadline(std::mt19937& gen);
};
