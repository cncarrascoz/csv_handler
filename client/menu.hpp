// client/menu.hpp: Menu system for handling client commands
#pragma once

#include <string>
#include <unordered_map>
#include <functional>
#include <vector>
#include <memory>
#include "csv_client.hpp"

namespace client {

// ClientMenu class: Handles command dispatching for the client interface
class ClientMenu {
public:
    // Command handler typedef - function taking tokenized command and reference to client
    using CommandHandler = std::function<bool(const std::vector<std::string>&, CsvClient&)>;
    
    // Constructor
    ClientMenu();
    
    // Register a command handler
    void registerCommand(const std::string& command, CommandHandler handler, 
                         const std::string& description);
    
    // Process a command line
    bool processCommand(const std::string& command_line, CsvClient& client);
    
    // Display the menu
    void displayMenu() const;
    
    // Split a command line into tokens
    static std::vector<std::string> tokenizeCommand(const std::string& command_line);
    
    // Helper to check if a command has enough arguments
    static bool checkArgCount(const std::vector<std::string>& tokens, size_t min_args, size_t max_args = SIZE_MAX);
    
private:
    struct CommandInfo {
        CommandHandler handler;
        std::string description;
    };
    
    // Map of command name to handler and description
    std::unordered_map<std::string, CommandInfo> command_handlers_;
};

// Function to create and setup the client menu with all commands
std::unique_ptr<ClientMenu> createClientMenu();

} // namespace client
