// server/main.cpp: Minimal main entry point for the gRPC server.
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

// Include the service implementation header
#include "network/csv_service_impl.hpp"

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
    
    // Simple command interface
    while (true) {
        std::cout << "\nServer commands:\n";
        std::cout << "1. list - List loaded files\n";
        std::cout << "2. exit - Shutdown server\n";
        std::cout << "> ";
        
        std::string command;
        std::getline(std::cin, command);
        
        if (command == "list") {
            std::cout << "\nLoaded files:\n";
            for (const auto& entry : service.loaded_files) {
                if (!entry.second.column_names.empty()) {
                    auto it = entry.second.columns.find(entry.second.column_names[0]);
                    if (it != entry.second.columns.end() && !it->second.empty()) {
                        std::cout << "- " << entry.first << " (";
                        std::cout << entry.second.column_names.size() << " columns, ";
                        std::cout << it->second.size() << " rows)\n";
                    } else {
                        std::cout << "- " << entry.first << " (0 rows)\n";
                    }
                } else {
                    std::cout << "- " << entry.first << " (0 rows)\n";
                }
            }
        } else if (command == "exit") {
            break;
        }
    }
    
    server->Shutdown();
}

int main(int argc, char** argv) {
    std::string server_address("0.0.0.0:50051");
    if (argc > 1) {
        server_address = argv[1];
    }
    RunServer(server_address);
    return 0;
}