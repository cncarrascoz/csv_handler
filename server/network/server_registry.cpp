#include "server_registry.hpp"
#include <iostream>
#include <algorithm>
#include <string>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "proto/csv_service.grpc.pb.h"
#include "proto/csv_service.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using csvservice::CsvService;
using csvservice::Empty;
using csvservice::ClusterStatusResponse;

namespace network {

// Singleton instance
ServerRegistry& ServerRegistry::instance() {
    static ServerRegistry instance;
    return instance;
}

// Constructor
ServerRegistry::ServerRegistry() 
    : running_(false), 
      rng_(std::random_device()()) {
    // Initialize with empty values
}

// Register this server
void ServerRegistry::register_self(const std::string& server_address) {
    std::lock_guard<std::mutex> lock(mutex_);
    self_address_ = server_address;
    
    // If no leader yet, become the leader
    if (leader_address_.empty()) {
        leader_address_ = self_address_;
        std::cout << "Server " << self_address_ << " is now the leader (initial)" << std::endl;
    }
    
    // Add self to last_seen
    last_seen_[self_address_] = std::chrono::steady_clock::now();
}

// Register a peer server
void ServerRegistry::register_peer(const std::string& peer_address) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Don't add duplicates
    if (std::find(peer_addresses_.begin(), peer_addresses_.end(), peer_address) == peer_addresses_.end()) {
        peer_addresses_.push_back(peer_address);
        last_seen_[peer_address] = std::chrono::steady_clock::now();
        consecutive_failures_[peer_address] = 0; // Initialize failure count
        
        std::cout << "Registered peer server: " << peer_address << std::endl;
        
        // Notify about server list change
        if (server_list_change_callback_) {
            std::vector<std::string> all_servers = get_all_servers();
            server_list_change_callback_(all_servers);
        }
    }
}

// Unregister a server
void ServerRegistry::unregister_server(const std::string& server_address) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Remove from peer addresses if present
    auto it = std::find(peer_addresses_.begin(), peer_addresses_.end(), server_address);
    if (it != peer_addresses_.end()) {
        peer_addresses_.erase(it);
        std::cout << "Unregistered server: " << server_address << std::endl;
    }
    
    // Remove from last_seen
    last_seen_.erase(server_address);
    
    // Remove from consecutive_failures
    consecutive_failures_.erase(server_address);
    
    // If the leader went down, trigger election
    if (server_address == leader_address_) {
        std::cout << "Leader " << leader_address_ << " went down, triggering election" << std::endl;
        leader_address_.clear();
        try_claim_leadership();
    }
    
    // Notify about server list change
    if (server_list_change_callback_) {
        std::vector<std::string> all_servers = get_all_servers();
        server_list_change_callback_(all_servers);
    }
}

// Get the current leader
std::string ServerRegistry::get_leader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return leader_address_;
}

// Check if this server is the leader
bool ServerRegistry::is_leader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !self_address_.empty() && self_address_ == leader_address_;
}

// Get this server's registered address
std::string ServerRegistry::get_self_address() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return self_address_;
}

// Start the health check and leader election threads
void ServerRegistry::start() {
    // Start background threads only if they aren't already running.
    // Peer registration now happens exclusively via command-line args in main.cpp
    start_time_ = std::chrono::steady_clock::now(); // Record start time for grace period
    if (!running_.exchange(true)) { 
        std::cout << "Starting ServerRegistry background threads..." << std::endl;
        health_check_thread_ = std::thread(&ServerRegistry::health_check_thread, this);

        // Start the leader election thread
        leader_election_thread_ = std::thread(&ServerRegistry::leader_election_thread, this);

        std::cout << "Server registry started" << std::endl;
    }
}

// Stop the health check and leader election threads
void ServerRegistry::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    // Wait for threads to finish
    if (health_check_thread_.joinable()) {
        health_check_thread_.join();
    }
    
    if (leader_election_thread_.joinable()) {
        leader_election_thread_.join();
    }
    
    std::cout << "Server registry stopped" << std::endl;
}

// Get all known server addresses
std::vector<std::string> ServerRegistry::get_all_servers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::string> all_servers;
    all_servers.reserve(peer_addresses_.size() + 1);
    
    // Add self if registered
    if (!self_address_.empty()) {
        all_servers.push_back(self_address_);
    }
    
    // Add peers
    all_servers.insert(all_servers.end(), peer_addresses_.begin(), peer_addresses_.end());
    
    return all_servers;
}

// Get count of active servers
size_t ServerRegistry::active_server_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Count self if registered
    size_t count = self_address_.empty() ? 0 : 1;
    
    // Add number of peers
    count += peer_addresses_.size();
    
    return count;
}

// Set callback for leader change events
void ServerRegistry::set_leader_change_callback(std::function<void(const std::string&)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    leader_change_callback_ = callback;
}

// Set callback for server list change events
void ServerRegistry::set_server_list_change_callback(std::function<void(const std::vector<std::string>&)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    server_list_change_callback_ = callback;
}

// Get all peer addresses (excluding self)
std::vector<std::string> ServerRegistry::get_peer_addresses() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return peer_addresses_;
}

// Set the Raft node reference
void ServerRegistry::set_raft_node(std::shared_ptr<RaftNode> raft_node) {
    std::lock_guard<std::mutex> lock(mutex_);
    raft_node_ = raft_node;
    
    // Set up a callback to update the leader when Raft consensus changes
    if (raft_node_) {
        std::cout << "ServerRegistry: Raft node set successfully" << std::endl;
    }
}

// Update leader based on Raft consensus
void ServerRegistry::update_leader_from_raft(const std::string& leader_address, uint64_t term) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Only update if the leader has changed
    if (leader_address_ != leader_address) {
        std::cout << "ServerRegistry: Leader updated from Raft consensus to " 
                  << leader_address << " (term " << term << ")" << std::endl;
        
        leader_address_ = leader_address;
        
        // Notify about leader change
        if (leader_change_callback_) {
            leader_change_callback_(leader_address_);
        }
    }
}

// Health check thread function
void ServerRegistry::health_check_thread() {
    const auto check_interval = std::chrono::seconds(3);
    const auto rpc_deadline_ms = std::chrono::milliseconds(500);
    const auto failure_threshold = 3; // Number of consecutive failures before unregistering
    const auto grace_period = std::chrono::seconds(15); // Initial grace period

    while (running_) {
        std::this_thread::sleep_for(check_interval);

        std::vector<std::string> peers;
        std::string current_leader;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            peers = peer_addresses_;
            current_leader = leader_address_;
        }

        bool past_grace_period = (now - start_time_) > grace_period;

        if (!past_grace_period) {
            std::cout << "Grace period active, skipping timeout checks for now..." << std::endl;
        }

        std::vector<std::string> servers_to_unregister;

        for (const auto& peer : peers) {
            // Create a temporary stub for the health check
            auto channel = grpc::CreateChannel(peer, grpc::InsecureChannelCredentials());
            auto stub = CsvService::NewStub(channel);

            Empty request;
            ClusterStatusResponse response;
            ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + rpc_deadline_ms);

            Status status = stub->GetClusterStatus(&context, request, &response);

            std::lock_guard<std::mutex> lock(mutex_);
            if (status.ok()) {
                // Peer is healthy, update last seen time and reset failure count
                last_seen_[peer] = now;
                consecutive_failures_[peer] = 0; // Reset failure count on success
                // std::cout << "Health check successful for peer " << peer << std::endl; // Optional: Verbose success log
            } else {
                // Peer failed health check
                std::cerr << "Health check failed for peer " << peer << ": " << status.error_message() << std::endl;
                
                // Increment consecutive failure count
                consecutive_failures_[peer]++;
                std::cerr << "Consecutive failures for " << peer << ": " << consecutive_failures_[peer] << std::endl;

                // Check if we should unregister this peer (past grace period AND failure threshold met)
                if (past_grace_period && consecutive_failures_[peer] >= failure_threshold) {
                    std::cerr << "Server " << peer << " exceeded failure threshold (" 
                              << consecutive_failures_[peer] << " >= " << failure_threshold 
                              << ") after grace period. Marking for unregistration." << std::endl;
                    servers_to_unregister.push_back(peer);
                } else if (!past_grace_period) {
                    std::cerr << "Still within grace period, not unregistering " << peer << " yet." << std::endl;
                } else {
                     std::cerr << "Failure count for " << peer << " (" << consecutive_failures_[peer] << ") below threshold (" << failure_threshold << ")." << std::endl;
                }
            }
        }

        // Unregister marked servers (outside the peer loop to avoid iterator invalidation)
        for (const auto& server_addr : servers_to_unregister) {
            unregister_server(server_addr); // This locks mutex internally
            // Also remove from consecutive_failures map when unregistered
            std::lock_guard<std::mutex> lock(mutex_);
            consecutive_failures_.erase(server_addr);
        }
    }
}

// Leader election thread function
void ServerRegistry::leader_election_thread() {
    const auto check_interval = std::chrono::seconds(3);
    
    while (running_) {
        std::this_thread::sleep_for(check_interval);
        
        bool need_election = false;
        
        {
            std::lock_guard<std::mutex> lock(mutex_);
            
            // Check if we need an election
            need_election = leader_address_.empty();
        }
        
        if (need_election) {
            try_claim_leadership();
        }
    }
}

// Try to claim leadership
bool ServerRegistry::try_claim_leadership() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // If already a leader, nothing to do
    if (!leader_address_.empty()) {
        return false;
    }
    
    // If no self address, can't be leader
    if (self_address_.empty()) {
        return false;
    }
    
    // Simple algorithm: lowest address becomes leader
    std::vector<std::string> all_servers = get_all_servers();
    if (all_servers.empty()) {
        return false;
    }
    
    std::sort(all_servers.begin(), all_servers.end());
    std::string new_leader = all_servers[0];
    
    // If we're the lowest address, become leader
    if (new_leader == self_address_) {
        leader_address_ = self_address_;
        std::cout << "Server " << self_address_ << " is now the leader (election)" << std::endl;
        
        // Notify about leader change
        if (leader_change_callback_) {
            leader_change_callback_(leader_address_);
        }
        
        return true;
    } else {
        // Someone else should be leader
        leader_address_ = new_leader;
        std::cout << "Server " << new_leader << " is now the leader (election)" << std::endl;
        
        // Notify about leader change
        if (leader_change_callback_) {
            leader_change_callback_(leader_address_);
        }
        
        return false;
    }
}

} // namespace network
