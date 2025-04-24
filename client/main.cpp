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

// Function to check if the client can connect to the server
bool check_server_connection(CsvClient& client) {
    try {
        // Try to list files as a simple connection test
        if (!client.TestConnection()) {
            std::cerr << "Error: Failed to connect to the server. Please check the IP address and ensure the server is running." << std::endl;
            return false;
        }
        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Error: Failed to connect to the server: " << e.what() << std::endl;
        return false;
    }
}

int main(int argc, char** argv) {
    if (argc < 2) { // Need at least program name and server address
        show_usage(argv[0]);
        return 1;
    }

    // Collect all server addresses
    std::vector<std::string> server_addresses;
    int arg_index = 1;
    
    // Collect all arguments that look like server addresses (contain a colon)
    while (arg_index < argc && std::string(argv[arg_index]).find(':') != std::string::npos) {
        server_addresses.push_back(argv[arg_index]);
        arg_index++;
    }
    
    if (server_addresses.empty()) {
        std::cerr << "Error: No valid server addresses provided." << std::endl;
        show_usage(argv[0]);
        return 1;
    }
    
    // Create the client with multiple server addresses for fault tolerance
    CsvClient client(server_addresses);
    
    // Check if we can connect to at least one server
    if (!check_server_connection(client)) {
        return 1;
    }
    
    // Create the menu system
    auto menu = client::createClientMenu();

    // If arg_index reached the end, all arguments were server addresses -> interactive mode
    if (arg_index == argc) {
        // --- Interactive Mode --- 
        std::cout << "Connected to server cluster. Starting interactive mode." << std::endl;
        std::cout << "Type 'help' to see available commands." << std::endl;
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
        // Reconstruct the command and arguments from argv, starting after the server addresses
        std::string command_line;
        for (int i = arg_index; i < argc; ++i) {
            if (i > arg_index) command_line += " "; // Add space between command parts
            command_line += argv[i];
        }
        
        // Process the command using the menu system
        menu->processCommand(command_line, client);
    }

    return 0;
}