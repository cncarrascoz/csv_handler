#pragma once

#include "raft_node.hpp"
#include "rpc/IRaftRPC.hpp"
#include <string>
#include <memory>
#include <chrono>
#include <atomic>
#include <thread>
#include <functional>

/**
 * HeartbeatManager handles sending and receiving heartbeats between nodes
 * Used for leader election and cluster membership management
 * 
 * Note: This class is now a wrapper around the Raft consensus algorithm's
 * heartbeat mechanism, which uses AppendEntries RPCs with empty entries.
 */
class HeartbeatManager {
public:
    /**
     * Constructor
     * @param node_id ID of this node
     */
    explicit HeartbeatManager(const std::string& node_id);
    
    /**
     * Start sending heartbeats
     * @param interval_ms Interval between heartbeats in milliseconds
     */
    void start_heartbeats(int interval_ms = 100);
    
    /**
     * Stop sending heartbeats
     */
    void stop_heartbeats();
    
    /**
     * Send a single heartbeat
     * @param term Current term
     * @param is_leader Whether this node is the leader
     * @param last_committed_index Index of the last committed entry
     * @return True if heartbeat was sent successfully
     */
    bool send_heartbeat(uint64_t term, bool is_leader, uint64_t last_committed_index);

private:
    std::string node_id_;
    std::atomic<bool> running_{false};
    std::thread heartbeat_thread_;
    
    // Thread function for periodic heartbeats
    void heartbeat_loop(int interval_ms);
};
