#include "raft_server.hpp"
// This is a placeholder file for build purposes
// The real implementation will be added once protobuf generation is set up

// RaftServer stub implementation
RaftServer::RaftServer(std::shared_ptr<RaftNode> node, const std::string& address)
    : node_(node), server_address_(address) {
}

void RaftServer::start() {
    std::cout << "RaftServer stub: Would start server at " << server_address_ << std::endl;
}

void RaftServer::stop() {
    std::cout << "RaftServer stub: Would stop server at " << server_address_ << std::endl;
}

std::string RaftServer::address() const {
    return server_address_;
}
