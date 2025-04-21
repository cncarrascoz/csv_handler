#pragma once

#include "IRaftRPC.hpp"
#include <unordered_map>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>
#include <queue>
#include <condition_variable>

/**
 * Implementation of IRaftRPC using TCP sockets for communication
 */
class SocketRaftRPC : public IRaftRPC {
public:
    /**
     * Constructor
     * @param local_address The address of this node (host:port)
     * @param request_vote_handler Function to handle incoming vote requests
     * @param append_entries_handler Function to handle incoming append entries requests
     */
    SocketRaftRPC(
        const std::string& local_address,
        std::function<RequestVoteResponse(const RequestVoteRequest&)> request_vote_handler,
        std::function<AppendEntriesResponse(const AppendEntriesRequest&)> append_entries_handler);
    
    /**
     * Destructor
     */
    ~SocketRaftRPC();
    
    /**
     * Start the RPC server
     */
    void start() override;
    
    /**
     * Stop the RPC server
     */
    void stop() override;
    
    /**
     * Send a request for votes to a peer
     * @param peer_address The address of the peer
     * @param request The vote request
     * @return The response from the peer
     */
    RequestVoteResponse request_vote(const std::string& peer_address, const RequestVoteRequest& request) override;
    
    /**
     * Send a request to append entries to a peer's log
     * @param peer_address The address of the peer
     * @param request The append entries request
     * @return The response from the peer
     */
    AppendEntriesResponse append_entries(const std::string& peer_address, const AppendEntriesRequest& request) override;

private:
    std::string local_address_;
    std::function<RequestVoteResponse(const RequestVoteRequest&)> request_vote_handler_;
    std::function<AppendEntriesResponse(const AppendEntriesRequest&)> append_entries_handler_;
    
    // Server thread
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    
    // Client connections cache
    struct Connection {
        int socket_fd;
        std::chrono::steady_clock::time_point last_used;
    };
    
    std::mutex connections_mutex_;
    std::unordered_map<std::string, Connection> connections_;
    
    // Server implementation
    void server_loop();
    void handle_client(int client_socket);
    
    // Client implementation
    int connect_to_peer(const std::string& peer_address);
    void close_connection(const std::string& peer_address);
    
    // Helper methods
    std::pair<std::string, int> parse_address(const std::string& address);
    
    // Private helper methods to handle RPC requests
    RequestVoteResponse handle_request_vote(const RequestVoteRequest& request);
    AppendEntriesResponse handle_append_entries(const AppendEntriesRequest& request);
};
