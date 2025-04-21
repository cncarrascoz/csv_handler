#pragma once

#include "../core/LogEntry.hpp"
#include <string>
#include <vector>
#include <cstdint>

/**
 * Struct representing a request for votes in the Raft election process
 */
struct RequestVoteRequest {
    std::string candidate_id;
    uint64_t term;
    uint64_t last_log_index;
    uint64_t last_log_term;
    
    std::string serialize() const;
    static RequestVoteRequest deserialize(const std::string& serialized);
};

/**
 * Struct representing a response to a vote request
 */
struct RequestVoteResponse {
    uint64_t term;
    bool vote_granted;
    
    std::string serialize() const;
    static RequestVoteResponse deserialize(const std::string& serialized);
};

/**
 * Struct representing a request to append entries to the Raft log
 */
struct AppendEntriesRequest {
    std::string leader_id;
    uint64_t term;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    std::vector<LogEntry> entries;
    
    std::string serialize() const;
    static AppendEntriesRequest deserialize(const std::string& serialized);
};

/**
 * Struct representing a response to an append entries request
 */
struct AppendEntriesResponse {
    uint64_t term;
    bool success;
    uint64_t match_index; // The highest log entry known to be replicated
    
    std::string serialize() const;
    static AppendEntriesResponse deserialize(const std::string& serialized);
};

/**
 * Interface for RPC communication between Raft nodes
 */
class IRaftRPC {
public:
    virtual ~IRaftRPC() = default;
    
    /**
     * Send a request for votes to a peer
     * @param peer_address The address of the peer
     * @param request The vote request
     * @return The response from the peer
     */
    virtual RequestVoteResponse request_vote(const std::string& peer_address, const RequestVoteRequest& request) = 0;
    
    /**
     * Send a request to append entries to a peer's log
     * @param peer_address The address of the peer
     * @param request The append entries request
     * @return The response from the peer
     */
    virtual AppendEntriesResponse append_entries(const std::string& peer_address, const AppendEntriesRequest& request) = 0;
    
    /**
     * Handle an incoming request for votes
     * @param request The vote request
     * @return The response to send back
     */
    virtual RequestVoteResponse handle_request_vote(const RequestVoteRequest& request) = 0;
    
    /**
     * Handle an incoming request to append entries
     * @param request The append entries request
     * @return The response to send back
     */
    virtual AppendEntriesResponse handle_append_entries(const AppendEntriesRequest& request) = 0;
};
