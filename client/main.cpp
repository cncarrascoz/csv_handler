// client/main.cpp: Main entry point for the gRPC client.
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

// Include the client class and menu system
#include "csv_client.hpp"
#include "menu.hpp"

// Function to print usage instructions
void show_usage(const std::string& name) {
    std::cerr << "Usage: " << name << " <server_address> [command] [arguments...]"
              << std::endl << std::endl
              << "If only <server_address> is provided, enters interactive mode."
              << std::endl << std::endl
              << "Server address format: <IP_ADDRESS>:50051"
              << std::endl
              << "  - For local connections: localhost:50051"
              << std::endl
              << "  - For remote connections: 192.168.1.X:50051 (replace with actual IP)"
              << std::endl
              << "  - Ask the server administrator to run the 'ip' command to get the correct address"
              << std::endl << std::endl
              << "Commands (command-line or interactive):"
              << std::endl
              << "  upload <filename>                - Upload a CSV file"
              << std::endl
              << "  list                             - List files loaded on server"
              << std::endl
              << "  view <filename>                  - View file contents"
              << std::endl
              << "  sum <filename> <column_name>     - Compute sum of column values"
              << std::endl
              << "  avg <filename> <column_name>     - Compute average of column values"
              << std::endl
              << "  insert <filename> <val1,val2...> - Insert a new row"
              << std::endl
              << "  delete <filename> <row_index>    - Delete a row"
              << std::endl
              << "  exit                             - Exit interactive mode"
              << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 2) { // Need at least program name and server address
        show_usage(argv[0]);
        return 1;
    }

    std::string server_address = argv[1];

    // Create a channel to the server
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        server_address, grpc::InsecureChannelCredentials());

    // Create the client object
    CsvClient client(channel);
    
    // Create the menu system
    auto menu = client::createClientMenu();

    if (argc == 2) {
        // --- Interactive Mode --- 
        std::cout << "Connected to server at " << server_address << std::endl;
        std::cout << "Entered interactive mode." << std::endl;
        std::string user_command;
        
        // Main command loop
        bool continue_loop = true;
        while (continue_loop) {
            menu->displayMenu();
            
            if (!std::getline(std::cin, user_command)) {
                break; // Handle EOF or input error
            }
            
            continue_loop = menu->processCommand(user_command, client);
        }
        
        // Make sure to stop all display threads before exiting
        client.StopAllDisplayThreads();
    } else {
        // --- Command-Line Mode --- 
        // Reconstruct the command and arguments from argv
        std::string command_line;
        for (int i = 2; i < argc; ++i) {
            if (i > 2) command_line += " ";
            command_line += argv[i];
        }
        
        // Process the command using the menu system
        menu->processCommand(command_line, client);
    }

    return 0;
}