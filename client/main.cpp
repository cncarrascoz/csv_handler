// client/main.cpp: Main entry point for the gRPC client.
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>

// Include the client class and menu system
#include "csv_client.hpp"
#include "menu.hpp"

// Function to print usage instructions
void show_usage(const std::string& name) {
    std::cerr << "Usage: " << name << " <server_address> [server_address2] [server_address3] [command] [arguments...]"
              << std::endl << std::endl
              << "If only server addresses are provided, enters interactive mode."
              << std::endl << std::endl
              << "Server address format: <IP_ADDRESS>:50051"
              << std::endl
              << "  - For local connections: localhost:50051"
              << std::endl
              << "  - For remote connections: 192.168.1.X:50051 (replace with actual IP)"
              << std::endl
              << "  - For fault tolerance, provide multiple server addresses"
              << std::endl
              << "  - Example: " << name << " 127.0.0.1:50051 127.0.0.1:50052 127.0.0.1:50053"
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
            std::cerr << "Error: Failed to connect to any server. Please check the IP addresses and ensure at least one server is running." << std::endl;
            return false;
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return false;
    }
}

int main(int argc, char** argv) {
    // Check for minimum arguments
    if (argc < 2) {
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
    
    // If there are more arguments, treat them as commands
    if (arg_index < argc) {
        std::string command = argv[arg_index++];
        
        if (command == "upload" && arg_index < argc) {
            std::string filename = argv[arg_index++];
            if (client.UploadCsv(filename)) {
                std::cout << "Upload successful." << std::endl;
            }
        } else if (command == "list") {
            client.ListFiles();
        } else if (command == "view" && arg_index < argc) {
            std::string filename = argv[arg_index++];
            client.ViewFile(filename);
        } else if (command == "sum" && arg_index + 1 < argc) {
            std::string filename = argv[arg_index++];
            std::string column_name = argv[arg_index++];
            client.ComputeSum(filename, column_name);
        } else if (command == "avg" && arg_index + 1 < argc) {
            std::string filename = argv[arg_index++];
            std::string column_name = argv[arg_index++];
            client.ComputeAverage(filename, column_name);
        } else if (command == "insert" && arg_index + 1 < argc) {
            std::string filename = argv[arg_index++];
            std::string row_data = argv[arg_index++];
            if (client.InsertRow(filename, row_data)) {
                std::cout << "Row inserted successfully." << std::endl;
            }
        } else if (command == "delete" && arg_index + 1 < argc) {
            std::string filename = argv[arg_index++];
            int row_index = std::stoi(argv[arg_index++]);
            if (client.DeleteRow(filename, row_index)) {
                std::cout << "Row deleted successfully." << std::endl;
            }
        } else {
            std::cerr << "Invalid command or missing arguments." << std::endl;
            show_usage(argv[0]);
            return 1;
        }
    } else {
        // Interactive mode
        std::cout << "Connected to server cluster. Starting interactive mode." << std::endl;
        std::cout << "Type 'help' to see available commands." << std::endl;
        
        // Start the menu system
        auto menu = client::createClientMenu();
        menu->processCommand("help", client);
        
        // Main command loop
        bool continue_loop = true;
        std::string user_command;
        
        while (continue_loop) {
            std::cout << "> ";
            if (!std::getline(std::cin, user_command)) {
                break; // Handle EOF or input error
            }
            
            continue_loop = menu->processCommand(user_command, client);
        }
        
        // Make sure to stop all display threads before exiting
        client.StopAllDisplayThreads();
    }
    
    return 0;
}