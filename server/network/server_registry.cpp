#include "server_registry.hpp"
#include <iostream>
#include <algorithm>
#include <string>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "../proto/csv_service.grpc.pb.h"
#include "../proto/csv_service.pb.h"

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

// Health check thread function
void ServerRegistry::health_check_thread() {
    const auto check_interval = std::chrono::seconds(3);
    const auto rpc_deadline_ms = std::chrono::milliseconds(500);
    const auto timeout_duration = std::chrono::seconds(10); // Must be >> check_interval
    const auto grace_period = std::chrono::seconds(15); // Initial grace period

    while (running_) {
        std::this_thread::sleep_for(check_interval);

        std::vector<std::string> peers;
        std::string current_leader;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            peers = peer_addresses_; // Copy to avoid holding lock during RPC
            current_leader = leader_address_; // Copy for timeout check
        }

        // Check connectivity to peers
        for (const auto& peer : peers) {
            try {
                auto channel = grpc::CreateChannel(peer, grpc::InsecureChannelCredentials());

                auto stub = CsvService::NewStub(channel);

                Empty request;
                ClusterStatusResponse response;
                ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + rpc_deadline_ms);

                Status status = stub->GetClusterStatus(&context, request, &response);

                if (status.ok()) {
                    // Update last seen timestamp on successful ping
                    std::lock_guard<std::mutex> lock(mutex_);
                    last_seen_[peer] = std::chrono::steady_clock::now();
                } else {
                    // Log the failure but don't update last_seen_
                    std::cerr << "Health check failed for peer " << peer << ": " << status.error_message() << std::endl;
                }
            } catch (const std::exception& e) {
                std::cerr << "Exception during health check for peer " << peer << ": " << e.what() << std::endl;
            }
        }

        // Check for timed-out peers
        const auto now = std::chrono::steady_clock::now();
        if (now < start_time_ + grace_period) {
            std::cout << "Grace period active, skipping timeout checks for now..." << std::endl;
            continue; // Skip timeout logic during grace period
        }

        std::vector<std::string> servers_to_unregister;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            // Iterate through registered peers to check for timeouts
            // Note: We iterate through peer_addresses_ and check last_seen_.
            // This ensures we only consider peers that *should* be active.
            for (const auto& server_addr : peer_addresses_) {
                // Don't time out self
                if (server_addr == self_address_) {
                    continue;
                }

                // Check if the peer has ever been seen and if it has timed out
                auto it = last_seen_.find(server_addr);
                if (it != last_seen_.end()) {
                    // Peer has been seen before, check the time
                    const auto last_seen_time = it->second;
                    if (now - last_seen_time > timeout_duration) {
                        std::cout << "Server " << server_addr << " timed out (last seen > " 
                                  << std::chrono::duration_cast<std::chrono::seconds>(timeout_duration).count() 
                                  << "s ago)." << std::endl;
                        servers_to_unregister.push_back(server_addr);
                        // Note: We don't erase from last_seen_ here, unregister_server handles that
                    }
                } else {
                    // Peer has never been successfully contacted. Don't time it out yet.
                    // The health check loop will keep trying to contact it.
                    // Optional: Could add a counter for initial connection failures if needed.
                    std::cout << "Peer " << server_addr << " has not responded to initial health checks yet." << std::endl;
                }
            }
        }

        // Unregister timed-out servers outside the lock
        bool leader_timed_out = false;
        for (const auto& server_address : servers_to_unregister) { 
            unregister_server(server_address);
            if (server_address == current_leader) {
                leader_timed_out = true;
            }
        }
        // If the leader specifically timed out, trigger an election check immediately
        // The leader_election_thread will handle the actual election logic
        if (leader_timed_out) {
             std::cout << "Leader " << current_leader << " timed out, ensuring election check." << std::endl;
             // No explicit action needed here, unregister_server already cleared leader_address_ 
             // if the unregistered server was the leader. leader_election_thread will pick it up.
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
