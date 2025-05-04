#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <random>
#include <functional>
#include <map>

namespace network {

/**
 * ServerRegistry maintains information about all servers in the cluster
 * and handles leader election when servers go down.
 */
class ServerRegistry {
public:
    // Singleton access
    static ServerRegistry& instance();
    
    // Register this server with the registry
    void register_self(const std::string& server_address);
    
    // Register a peer server
    void register_peer(const std::string& peer_address);
    
    // Unregister a server (e.g., when it's detected as down)
    void unregister_server(const std::string& server_address);
    
    // Get the current leader address
    std::string get_leader() const;
    
    // Check if this server is the leader
    bool is_leader() const;
    
    // Get the address registered for this server instance
    std::string get_self_address() const;
    
    // Start the health check and leader election threads
    void start();
    
    // Stop the health check and leader election threads
    void stop();
    
    // Get all known server addresses
    std::vector<std::string> get_all_servers() const;
    
    // Get all peer server addresses (excluding self)
    std::vector<std::string> get_peer_addresses() const;
    
    // Get count of active servers
    size_t active_server_count() const;
    
    // Set callback for leader change events
    void set_leader_change_callback(std::function<void(const std::string&)> callback);
    
    // Set callback for server list change events
    void set_server_list_change_callback(std::function<void(const std::vector<std::string>&)> callback);

private:
    // Private constructor for singleton
    ServerRegistry();
    
    // Prevent copying
    ServerRegistry(const ServerRegistry&) = delete;
    ServerRegistry& operator=(const ServerRegistry&) = delete;
    
    // Health check thread function
    void health_check_thread();
    
    // Leader election thread function
    void leader_election_thread();
    
    // Try to claim leadership
    bool try_claim_leadership();
    
    // Data members
    std::string self_address_;
    std::string leader_address_;
    std::vector<std::string> peer_addresses_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_seen_;
    
    // Thread control
    std::atomic<bool> running_;
    std::thread health_check_thread_;
    std::thread leader_election_thread_;
    
    // Mutex for thread safety
    mutable std::mutex mutex_;
    
    // Random number generator for election timeouts
    std::mt19937 rng_;
    
    // Timestamp for grace period
    std::chrono::steady_clock::time_point start_time_;
    
    // Map to track consecutive health check failures
    std::map<std::string, int> consecutive_failures_;

    // Callbacks
    std::function<void(const std::string&)> leader_change_callback_;
    std::function<void(const std::vector<std::string>&)> server_list_change_callback_;
};

} // namespace network
