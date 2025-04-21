#pragma once

#include "../core/LogEntry.hpp"
#include <string>
#include <vector>
#include <functional>

// Use the raft namespace
using raft::LogEntry;

/**
 * Request for RequestVote RPC
 */
struct RequestVoteRequest {
    uint64_t term;              // Candidate's term
    std::string candidate_id;   // Candidate requesting vote
    uint64_t last_log_index;    // Index of candidate's last log entry
    uint64_t last_log_term;     // Term of candidate's last log entry
    
    std::string serialize() const;
    static RequestVoteRequest deserialize(const std::string& data);
};

/**
 * Response for RequestVote RPC
 */
struct RequestVoteResponse {
    uint64_t term;          // Current term, for candidate to update itself
    bool vote_granted;      // True means candidate received vote
    
    std::string serialize() const;
    static RequestVoteResponse deserialize(const std::string& data);
};

/**
 * Request for AppendEntries RPC
 */
struct AppendEntriesRequest {
    uint64_t term;              // Leader's term
    std::string leader_id;      // So follower can redirect clients
    uint64_t prev_log_index;    // Index of log entry immediately preceding new ones
    uint64_t prev_log_term;     // Term of prev_log_index entry
    std::vector<LogEntry> entries; // Log entries to store (empty for heartbeat)
    uint64_t leader_commit;     // Leader's commit index
    
    std::string serialize() const;
    static AppendEntriesRequest deserialize(const std::string& data);
};

/**
 * Response for AppendEntries RPC
 */
struct AppendEntriesResponse {
    uint64_t term;          // Current term, for leader to update itself
    bool success;           // True if follower contained entry matching prev_log_index and prev_log_term
    uint64_t match_index;   // The highest log entry index known to be replicated on the follower
    
    std::string serialize() const;
    static AppendEntriesResponse deserialize(const std::string& data);
};

/**
 * Interface for Raft RPC communication
 */
class IRaftRPC {
public:
    virtual ~IRaftRPC() = default;
    
    /**
     * Start the RPC server
     */
    virtual void start() = 0;
    
    /**
     * Stop the RPC server
     */
    virtual void stop() = 0;
    
    /**
     * Send a RequestVote RPC to a peer
     * @param peer_address The address of the peer
     * @param request The request to send
     * @return The response from the peer
     */
    virtual RequestVoteResponse request_vote(const std::string& peer_address, 
                                            const RequestVoteRequest& request) = 0;
    
    /**
     * Send an AppendEntries RPC to a peer
     * @param peer_address The address of the peer
     * @param request The request to send
     * @return The response from the peer
     */
    virtual AppendEntriesResponse append_entries(const std::string& peer_address, 
                                               const AppendEntriesRequest& request) = 0;
};
