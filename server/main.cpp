// server/main.cpp: Main entry point for the gRPC server.
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

// Include the service implementation header and menu system
#include "network/csv_service_impl.hpp"
#include "menu.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// Function to run the server
void RunServer(const std::string& server_address) {
    CsvServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    
    // Create the menu system
    auto menu = server::createServerMenu();
    
    // Simple command interface with new menu system
    std::string command;
    bool continue_loop = true;
    
    while (continue_loop) {
        menu->displayMenu();
        
        if (!std::getline(std::cin, command)) {
            break; // Handle EOF or input error
        }
        
        // Process the command using the menu system
        continue_loop = menu->processCommand(command, service);
    }
    
    // Shutdown initiated by command or by user exiting
    std::cout << "Shutting down server..." << std::endl;
    server->Shutdown();
    std::cout << "Server shutdown complete." << std::endl;
}

int main(int argc, char** argv) {
    std::string server_address("0.0.0.0:50051");
    if (argc > 1) {
        server_address = argv[1];
    }
    RunServer(server_address);
    return 0;
}