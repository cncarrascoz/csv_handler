// server/main.cpp: Main entry point for the gRPC server.
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
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
void RunServer(const std::string& server_address) {
    // Create the service implementation with thread-safe storage
    CsvServiceImpl service;
    std::unique_ptr<Server> server;
    
    // Start the gRPC server in a separate thread
    std::thread grpc_thread(RunGrpcServer, server_address, &service, &server);
    
    // Create the menu system for server administration
    auto menu = server::createServerMenu();
    
    // Simple command interface with new menu system
    std::string command;
    bool continue_loop = true;
    
    std::cout << "Server started. Multiple clients can now connect." << std::endl;
    std::cout << "Use the server menu to monitor and control the server." << std::endl;
    
    while (continue_loop) {
        menu->displayMenu();
        
        if (!std::getline(std::cin, command)) {
            break; // Handle EOF or input error
        }
        
        // Process the command using the menu system
        continue_loop = menu->processCommand(command, service);
    }
    
    // Signal the server thread to shutdown
    {
        std::lock_guard<std::mutex> lock(server_mutex);
        server_running.store(false);
    }
    server_cv.notify_one();
    
    // Wait for the server thread to complete
    grpc_thread.join();
    
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