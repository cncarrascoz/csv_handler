// server/menu.hpp: Menu system for handling server commands
#pragma once

#include <string>
#include <unordered_map>
#include <functional>
#include <vector>
#include <memory>
#include "network/csv_service_impl.hpp"

namespace server {

// ServerMenu class: Handles command dispatching for the server interface
class ServerMenu {
public:
    // Command handler typedef - function taking tokenized command and reference to service
    using CommandHandler = std::function<bool(const std::vector<std::string>&, CsvServiceImpl&)>;
    
    // Constructor
    ServerMenu();
    
    // Register a command handler
    void registerCommand(const std::string& command, CommandHandler handler, 
                         const std::string& description);
    
    // Process a command line
    bool processCommand(const std::string& command_line, CsvServiceImpl& service);
    
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

// Function to create and setup the server menu with all commands
std::unique_ptr<ServerMenu> createServerMenu();

} // namespace server
