// server/main.cpp: Main entry point for the gRPC server.
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <vector>
#include <grpcpp/grpcpp.h>

// Include the service implementation header and menu system
#include "network/csv_service_impl.hpp"
#include "menu.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// Global variables for server control
std::atomic<bool> server_running(true);
std::mutex server_mutex;
std::condition_variable server_cv;

// Function to run the gRPC server in a separate thread
void RunGrpcServer(const std::string& server_address, CsvServiceImpl* service, std::unique_ptr<Server>* server_ptr) {
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);

    *server_ptr = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;
    
    // Wait until signaled to shutdown
    std::unique_lock<std::mutex> lock(server_mutex);
    server_cv.wait(lock, []{ return !server_running.load(); });
    
    // Shutdown initiated
    std::cout << "Shutting down gRPC server..." << std::endl;
    (*server_ptr)->Shutdown();
    std::cout << "gRPC server shutdown complete." << std::endl;
}

// Function to run the server
void RunServer(const std::string& server_address, bool distributed_mode = false, 
              const std::vector<std::string>& peer_addresses = {}) {
    
    std::unique_ptr<CsvServiceImpl> service;
    
    if (distributed_mode) {
        // Extract node ID from server address (use the port number as ID)
        std::string node_id = server_address.substr(server_address.find(':') + 1);
        std::string storage_path = "./raft_data_" + node_id;
        
        std::cout << "Starting server in distributed mode with Raft consensus" << std::endl;
        std::cout << "Node ID: " << node_id << std::endl;
        std::cout << "Storage path: " << storage_path << std::endl;
        std::cout << "Peer addresses: ";
        for (const auto& addr : peer_addresses) {
            std::cout << addr << " ";
        }
        std::cout << std::endl;
        
        // Create the service with Raft consensus
        service = std::make_unique<CsvServiceImpl>(node_id, peer_addresses, storage_path);
    } else {
        // Create the service with in-memory state machine (non-distributed)
        service = std::make_unique<CsvServiceImpl>();
        std::cout << "Starting server in non-distributed mode" << std::endl;
    }
    
    std::unique_ptr<Server> server;
    
    // Start the gRPC server in a separate thread
    std::thread grpc_thread(RunGrpcServer, server_address, service.get(), &server);
    
    // Start the menu system for interactive control
    std::cout << "Server started. Enter 'help' for available commands." << std::endl;
    
    // Create a menu system for the server
    auto menu = server::createServerMenu();
    
    // Main command loop
    std::string user_command;
    bool continue_loop = true;
    
    while (continue_loop && server_running.load()) {
        std::cout << "> ";
        if (!std::getline(std::cin, user_command)) {
            break; // Handle EOF
        }
        
        continue_loop = menu->processCommand(user_command, *service);
    }
    
    // Signal the server to shutdown
    {
        std::lock_guard<std::mutex> lock(server_mutex);
        server_running.store(false);
    }
    server_cv.notify_all();
    
    // Wait for the gRPC thread to finish
    grpc_thread.join();
    
    std::cout << "Server shutdown complete." << std::endl;
}

int main(int argc, char** argv) {
    std::string server_address("0.0.0.0:50051");
    bool distributed_mode = false;
    std::vector<std::string> peer_addresses;
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--distributed" || arg == "-d") {
            distributed_mode = true;
            std::cout << "Distributed mode enabled" << std::endl;
        } else if (arg == "--peer" || arg == "-p") {
            if (i + 1 < argc) {
                peer_addresses.push_back(argv[++i]);
            } else {
                std::cerr << "Error: --peer requires an address argument" << std::endl;
                return 1;
            }
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options] [address]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --distributed, -d     Enable distributed mode with Raft consensus" << std::endl;
            std::cout << "  --peer ADDR, -p ADDR  Add a peer address (can be specified multiple times)" << std::endl;
            std::cout << "  --help, -h            Show this help message" << std::endl;
            std::cout << "  address               Server address to listen on (default: 0.0.0.0:50051)" << std::endl;
            return 0;
        } else if (i == argc - 1) {
            // Last argument is the server address
            server_address = arg;
        }
    }
    
    RunServer(server_address, distributed_mode, peer_addresses);
    return 0;
}