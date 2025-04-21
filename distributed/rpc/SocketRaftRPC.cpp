#include "SocketRaftRPC.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <string.h>
#include <fcntl.h>
#include <chrono>

// Helper function to parse host:port address
std::pair<std::string, int> SocketRaftRPC::parse_address(const std::string& address) {
    size_t colon_pos = address.find(':');
    if (colon_pos == std::string::npos) {
        return {address, 50051}; // Default port
    }
    
    std::string host = address.substr(0, colon_pos);
    int port = std::stoi(address.substr(colon_pos + 1));
    return {host, port};
}

SocketRaftRPC::SocketRaftRPC(
    const std::string& local_address,
    std::function<RequestVoteResponse(const RequestVoteRequest&)> request_vote_handler,
    std::function<AppendEntriesResponse(const AppendEntriesRequest&)> append_entries_handler)
    : local_address_(local_address),
      request_vote_handler_(request_vote_handler),
      append_entries_handler_(append_entries_handler) {
}

SocketRaftRPC::~SocketRaftRPC() {
    stop();
}

void SocketRaftRPC::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }
    
    // Start the server thread
    server_thread_ = std::thread(&SocketRaftRPC::server_loop, this);
    
    std::cout << "RPC server started on " << local_address_ << std::endl;
}

void SocketRaftRPC::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    // Wait for the server thread to finish
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    
    // Close all connections
    std::lock_guard<std::mutex> lock(connections_mutex_);
    for (auto& pair : connections_) {
        close(pair.second.socket_fd);
    }
    connections_.clear();
    
    std::cout << "RPC server stopped" << std::endl;
}

RequestVoteResponse SocketRaftRPC::request_vote(const std::string& peer_address, const RequestVoteRequest& request) {
    try {
        // Connect to the peer
        int socket_fd = connect_to_peer(peer_address);
        if (socket_fd < 0) {
            throw std::runtime_error("Failed to connect to peer");
        }
        
        // Send the request type (1 = RequestVote)
        uint8_t request_type = 1;
        send(socket_fd, &request_type, sizeof(request_type), 0);
        
        // Send the serialized request
        std::string serialized = request.serialize();
        uint32_t length = serialized.size();
        send(socket_fd, &length, sizeof(length), 0);
        send(socket_fd, serialized.c_str(), length, 0);
        
        // Receive the response
        uint32_t response_length;
        if (recv(socket_fd, &response_length, sizeof(response_length), 0) != sizeof(response_length)) {
            throw std::runtime_error("Failed to receive response length");
        }
        
        std::string response_str(response_length, '\0');
        if (recv(socket_fd, &response_str[0], response_length, 0) != response_length) {
            throw std::runtime_error("Failed to receive response");
        }
        
        // Deserialize the response
        return RequestVoteResponse::deserialize(response_str);
    } catch (const std::exception& e) {
        std::cerr << "Error in request_vote RPC to " << peer_address << ": " << e.what() << std::endl;
        
        // Close the connection on error
        close_connection(peer_address);
        
        // Return a default response (vote not granted)
        return {0, false};
    }
}

AppendEntriesResponse SocketRaftRPC::append_entries(const std::string& peer_address, const AppendEntriesRequest& request) {
    try {
        // Connect to the peer
        int socket_fd = connect_to_peer(peer_address);
        if (socket_fd < 0) {
            throw std::runtime_error("Failed to connect to peer");
        }
        
        // Send the request type (2 = AppendEntries)
        uint8_t request_type = 2;
        send(socket_fd, &request_type, sizeof(request_type), 0);
        
        // Send the serialized request
        std::string serialized = request.serialize();
        uint32_t length = serialized.size();
        send(socket_fd, &length, sizeof(length), 0);
        send(socket_fd, serialized.c_str(), length, 0);
        
        // Receive the response
        uint32_t response_length;
        if (recv(socket_fd, &response_length, sizeof(response_length), 0) != sizeof(response_length)) {
            throw std::runtime_error("Failed to receive response length");
        }
        
        std::string response_str(response_length, '\0');
        if (recv(socket_fd, &response_str[0], response_length, 0) != response_length) {
            throw std::runtime_error("Failed to receive response");
        }
        
        // Deserialize the response
        return AppendEntriesResponse::deserialize(response_str);
    } catch (const std::exception& e) {
        std::cerr << "Error in append_entries RPC to " << peer_address << ": " << e.what() << std::endl;
        
        // Close the connection on error
        close_connection(peer_address);
        
        // Return a default response (not successful)
        return {0, false, 0};
    }
}

// Private helper methods to handle RPC requests
// These are not part of the public interface
RequestVoteResponse SocketRaftRPC::handle_request_vote(const RequestVoteRequest& request) {
    return request_vote_handler_(request);
}

AppendEntriesResponse SocketRaftRPC::handle_append_entries(const AppendEntriesRequest& request) {
    return append_entries_handler_(request);
}

void SocketRaftRPC::server_loop() {
    // Parse the local address
    auto [host, port] = parse_address(local_address_);
    
    // Create the server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket" << std::endl;
        return;
    }
    
    // Set socket options
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Failed to set socket options" << std::endl;
        close(server_fd);
        return;
    }
    
    // Bind the socket
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Failed to bind server socket" << std::endl;
        close(server_fd);
        return;
    }
    
    // Listen for connections
    if (listen(server_fd, 10) < 0) {
        std::cerr << "Failed to listen on server socket" << std::endl;
        close(server_fd);
        return;
    }
    
    // Set the server socket to non-blocking
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);
    
    // Accept connections and handle requests
    while (running_) {
        // Accept a new connection
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_addr_len);
        
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No pending connections, sleep for a short time
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            } else {
                std::cerr << "Failed to accept connection: " << strerror(errno) << std::endl;
                continue;
            }
        }
        
        // Handle the client in a separate thread
        std::thread client_thread(&SocketRaftRPC::handle_client, this, client_fd);
        client_thread.detach();
    }
    
    // Close the server socket
    close(server_fd);
}

void SocketRaftRPC::handle_client(int client_socket) {
    try {
        // Receive the request type
        uint8_t request_type;
        if (recv(client_socket, &request_type, sizeof(request_type), 0) != sizeof(request_type)) {
            throw std::runtime_error("Failed to receive request type");
        }
        
        // Receive the request length
        uint32_t request_length;
        if (recv(client_socket, &request_length, sizeof(request_length), 0) != sizeof(request_length)) {
            throw std::runtime_error("Failed to receive request length");
        }
        
        // Receive the request data
        std::string request_str(request_length, '\0');
        if (recv(client_socket, &request_str[0], request_length, 0) != request_length) {
            throw std::runtime_error("Failed to receive request data");
        }
        
        // Handle the request based on its type
        std::string response_str;
        
        if (request_type == 1) {
            // RequestVote
            RequestVoteRequest request = RequestVoteRequest::deserialize(request_str);
            RequestVoteResponse response = handle_request_vote(request);
            response_str = response.serialize();
        } else if (request_type == 2) {
            // AppendEntries
            AppendEntriesRequest request = AppendEntriesRequest::deserialize(request_str);
            AppendEntriesResponse response = handle_append_entries(request);
            response_str = response.serialize();
        } else {
            throw std::runtime_error("Unknown request type");
        }
        
        // Send the response
        uint32_t response_length = response_str.size();
        send(client_socket, &response_length, sizeof(response_length), 0);
        send(client_socket, response_str.c_str(), response_length, 0);
    } catch (const std::exception& e) {
        std::cerr << "Error handling client: " << e.what() << std::endl;
    }
    
    // Close the client socket
    close(client_socket);
}

int SocketRaftRPC::connect_to_peer(const std::string& peer_address) {
    // Check if we already have a connection
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        auto it = connections_.find(peer_address);
        if (it != connections_.end()) {
            // Update the last used time
            it->second.last_used = std::chrono::steady_clock::now();
            return it->second.socket_fd;
        }
    }
    
    // Parse the peer address
    auto [host, port] = parse_address(peer_address);
    
    // Create a socket
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return -1;
    }
    
    // Set a timeout for the connection
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
    setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof tv);
    
    // Resolve the host
    struct hostent* server = gethostbyname(host.c_str());
    if (server == nullptr) {
        std::cerr << "Failed to resolve host: " << host << std::endl;
        close(socket_fd);
        return -1;
    }
    
    // Set up the server address
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    memcpy(&server_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    server_addr.sin_port = htons(port);
    
    // Connect to the server
    if (connect(socket_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to connect to " << peer_address << std::endl;
        close(socket_fd);
        return -1;
    }
    
    // Store the connection
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_[peer_address] = {socket_fd, std::chrono::steady_clock::now()};
    }
    
    return socket_fd;
}

void SocketRaftRPC::close_connection(const std::string& peer_address) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    auto it = connections_.find(peer_address);
    if (it != connections_.end()) {
        close(it->second.socket_fd);
        connections_.erase(it);
    }
}
