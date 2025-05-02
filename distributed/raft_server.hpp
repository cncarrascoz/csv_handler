#pragma once

#include "raft_node.hpp"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

// Forward declarations for generated gRPC code
namespace raft {
class RaftService;
}

/**
 * RaftServer: Exposes a RaftNode over gRPC for cluster communication
 * 
 * This server implements the RaftService defined in raft_service.proto,
 * translating between gRPC messages and internal RaftNode calls.
 */
class RaftServer {
public:
    /**
     * Constructor
     * @param node The RaftNode to expose via gRPC
     * @param address Server address in the format "address:port"
     */
    RaftServer(std::shared_ptr<RaftNode> node, const std::string& address);
    
    /**
     * Start the gRPC server
     */
    void start();
    
    /**
     * Stop the gRPC server
     */
    void stop();
    
    /**
     * Get the address of this server
     */
    std::string address() const;

private:
    std::shared_ptr<RaftNode> node_;
    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    
    // gRPC service implementation (defined in raft_server.cpp)
    class RaftServiceImpl;
    std::unique_ptr<RaftServiceImpl> service_;
};
