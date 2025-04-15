// client/main.cpp: Minimal main entry point for the gRPC client.
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

// Include the client class header
#include "csv_client.hpp"

// Function to print usage instructions
void show_usage(const std::string& name) {
    std::cerr << "Usage: " << name << " <server_address> [command] [filename]"
              << std::endl << std::endl
              << "If only <server_address> is provided, enters interactive mode."
              << std::endl << std::endl
              << "Commands (command-line or interactive):"
              << std::endl
              << "  upload <filename>   Upload a CSV file"
              << std::endl
              << "  list              List files loaded on server"
              << std::endl
              << "  exit              Exit interactive mode"
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

    if (argc == 2) {
        // --- Interactive Mode --- 
        std::cout << "Entered interactive mode." << std::endl;
        std::string user_command;
        while (true) {
            // Print the menu options each time
            std::cout << "\nClient commands:\n";
            std::cout << "1. upload <filename> - Upload a CSV file\n";
            std::cout << "2. list              - List loaded files on server\n";
            std::cout << "3. exit              - Exit client\n";
            std::cout << "> ";
            if (!std::getline(std::cin, user_command)) {
                break; // Handle EOF or input error
            }

            if (user_command.rfind("upload ", 0) == 0) {
                if (user_command.length() > 7) {
                    std::string filename = user_command.substr(7);
                    client.UploadCsv(filename);
                } else {
                    std::cerr << "Error: 'upload' command requires a filename." << std::endl;
                }
            } else if (user_command == "list") {
                client.ListFiles();
            } else if (user_command == "exit") {
                std::cout << "Exiting interactive mode." << std::endl;
                break;
            } else if (user_command.empty()) {
                // Ignore empty input
            } else {
                std::cerr << "Error: Unknown command '" << user_command << "'." << std::endl;
                std::cout << "Available commands: upload <file>, list, exit" << std::endl;
            }
        }
    } else {
        // --- Command-Line Mode --- 
        std::string command = argv[2];
        if (command == "upload") {
            if (argc != 4) {
                std::cerr << "Error: 'upload' command requires a filename." << std::endl;
                show_usage(argv[0]);
                return 1;
            }
            std::string filename = argv[3];
            client.UploadCsv(filename);
        } else if (command == "list") {
            if (argc != 3) {
                std::cerr << "Error: 'list' command does not take arguments." << std::endl;
                show_usage(argv[0]);
                return 1;
            }
            client.ListFiles();
        } else {
            std::cerr << "Error: Unknown command '" << command << "'." << std::endl;
            show_usage(argv[0]);
            return 1;
        }
    }

    return 0;
}