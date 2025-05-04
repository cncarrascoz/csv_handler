// server/main.cpp: Main entry point for the gRPC server.
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <csignal> // Include for signal handling

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h> // Added for reflection
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "proto/csv_service.grpc.pb.h"
#include "network/csv_service_impl.hpp"
#include "network/server_registry.hpp" // Added for ServerRegistry

#include <mutex> // Keep for potential future use if needed
#include <condition_variable> // Keep for potential future use if needed

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using csvservice::CsvService;
using network::CsvServiceImpl;

// Function to handle interactive server commands
void RunServerInteraction(Server* server, CsvServiceImpl* service) {
    std::string command;
    bool continue_loop = true;

    std::cout << "Server interaction thread started. Type 'help' for commands." << std::endl;

    while (continue_loop) {
        std::cout << "> ";
        if (!std::getline(std::cin, command)) {
            // EOF or error, attempt graceful shutdown
            std::cerr << "Input error or EOF detected. Shutting down server..." << std::endl;
            if (server) {
                server->Shutdown();
            }
            continue_loop = false;
            break;
        }

        if (command == "exit") {
            std::cout << "Shutdown command received. Initiating server shutdown..." << std::endl;
            if (server) {
                server->Shutdown(); // Signal main thread waiting on server->Wait()
            }
            continue_loop = false; // Exit the interaction loop
        } else if (command == "list") {
            // Directly call a method on the service to list files (needs implementation in CsvServiceImpl)
            // Example: service->ListManagedFiles(); // Assuming such a method exists
            std::cout << "List command received (requires CsvServiceImpl implementation)." << std::endl;
            // TODO: Implement a way for CsvServiceImpl to provide file list/stats to the console
        } else if (command == "stats") {
             std::cout << "Stats command received (requires CsvServiceImpl implementation)." << std::endl;
             // TODO: Implement a way for CsvServiceImpl to provide file list/stats to the console
        } else if (command == "help") {
            std::cout << "Available commands:" << std::endl;
            std::cout << "  exit   - Shutdown the server" << std::endl;
            std::cout << "  list   - List currently loaded files (TODO)" << std::endl;
            std::cout << "  stats  - Show statistics for files (TODO)" << std::endl;
            std::cout << "  help   - Display this help message" << std::endl;
        } else if (!command.empty()){
            std::cout << "Unknown command: " << command << ". Type 'help' for options." << std::endl;
        }
    }
    std::cout << "Server interaction thread finished." << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 2) { // Expect at least program name and self address
        std::cerr << "Usage: " << argv[0] << " <self_address> [peer_address_1] [peer_address_2] ..." << std::endl;
        std::cerr << "  Example: " << argv[0] << " localhost:50051 localhost:50052 localhost:50053" << std::endl;
        return 1;
    }

    // The first argument is the address this server will use and advertise
    std::string self_address = argv[1];

    // Validate self_address format (simple check for colon)
    // Extract port for binding to 0.0.0.0
    size_t colon_pos = self_address.find_last_of(':');
    std::string port_str = (colon_pos == std::string::npos) ? "" : self_address.substr(colon_pos + 1);
    if (port_str.empty()) {
        std::cerr << "Invalid self address format: " << self_address << ". Expected format: <hostname>:<port>" << std::endl;
        return 1;
    }
    std::string listening_address = "0.0.0.0:" + port_str;

    // Check if this server should be the leader (50051 is our designated leader)
    bool is_leader = (port_str == "50051");
    std::cout << "Server role: " << (is_leader ? "LEADER" : "FOLLOWER") << std::endl;
    
    // Create a new ServerRegistry instance with the self address and leader status
    network::ServerRegistry registry(self_address, is_leader);
    
    // Start the registry background threads
    registry.start();
    
    std::cout << "Server registry initialized and started" << std::endl;
    std::cout << "Self address: " << self_address << std::endl;
    std::cout << "Leader address: " << registry.get_leader() << std::endl;
    
    // Print all known servers
    auto all_servers = registry.get_all_servers();
    std::cout << "All known servers (" << all_servers.size() << "): ";
    for (const auto& server : all_servers) {
        std::cout << server << " ";
    }
    std::cout << std::endl;

    // Instantiate the service implementation
    CsvServiceImpl service(registry);

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the server address without auth
    builder.AddListeningPort(listening_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);

    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << listening_address << " (Advertised as " << self_address << ")" << std::endl;

    // Start server interaction thread, passing the server and service instances
    std::thread interaction_thread(RunServerInteraction, server.get(), &service);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();

    // Ensure interaction thread finishes before exiting main
    interaction_thread.join();

    return 0;
}