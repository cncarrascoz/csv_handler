// server/menu.cpp: Implementation of the server menu system
#include "menu.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <array>
#include <memory>

namespace server {

ServerMenu::ServerMenu() {}

void ServerMenu::registerCommand(const std::string& command, CommandHandler handler, 
                               const std::string& description) {
    command_handlers_[command] = {handler, description};
}

bool ServerMenu::processCommand(const std::string& command_line, CsvServiceImpl& service) {
    if (command_line.empty()) {
        return true; // Empty command line, nothing to do
    }
    
    auto tokens = tokenizeCommand(command_line);
    if (tokens.empty()) {
        return true; // No tokens, nothing to do
    }
    
    const std::string& command = tokens[0];
    auto it = command_handlers_.find(command);
    
    if (it == command_handlers_.end()) {
        std::cerr << "Error: Unknown command '" << command << "'." << std::endl;
        std::cout << "Type 'help' for a list of available commands." << std::endl;
        return true;
    }
    
    return it->second.handler(tokens, service);
}

void ServerMenu::displayMenu() const {
    std::cout << "\nServer commands:\n";
    
    // Calculate the maximum command length for alignment
    size_t max_cmd_length = 0;
    for (const auto& entry : command_handlers_) {
        max_cmd_length = std::max(max_cmd_length, entry.first.length());
    }
    
    // Convert map to vector for sorting
    std::vector<std::pair<std::string, CommandInfo>> sorted_cmds(
        command_handlers_.begin(), command_handlers_.end());
    
    // Sort alphabetically by command name
    std::sort(sorted_cmds.begin(), sorted_cmds.end(), 
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
    // Display each command with its description
    int i = 1;
    for (const auto& [cmd, info] : sorted_cmds) {
        std::cout << i << ". " << std::left << std::setw(max_cmd_length + 2) 
                  << cmd << "- " << info.description << std::endl;
        i++;
    }
    
    std::cout << "> ";
}

std::vector<std::string> ServerMenu::tokenizeCommand(const std::string& command_line) {
    std::vector<std::string> tokens;
    std::istringstream iss(command_line);
    std::string token;
    
    while (iss >> token) {
        tokens.push_back(token);
    }
    
    return tokens;
}

bool ServerMenu::checkArgCount(const std::vector<std::string>& tokens, 
                             size_t min_args, size_t max_args) {
    if (tokens.size() - 1 < min_args) {
        std::cerr << "Error: Command '" << tokens[0] 
                  << "' requires at least " << min_args << " argument(s)." 
                  << std::endl;
        return false;
    }
    
    if (tokens.size() - 1 > max_args) {
        std::cerr << "Error: Command '" << tokens[0] 
                  << "' accepts at most " << max_args << " argument(s)." 
                  << std::endl;
        return false;
    }
    
    return true;
}

// Individual command handlers
namespace commands {

bool handleList(const std::vector<std::string>& tokens, CsvServiceImpl& service) {
    if (!ServerMenu::checkArgCount(tokens, 0, 0)) {
        return true;
    }
    
    std::cout << "\nLoaded files:\n";
    
    // Acquire shared lock for reading
    std::shared_lock<std::shared_mutex> lock(service.files_mutex);
    
    // Use get_loaded_files() instead of directly accessing loaded_files
    const auto& files = service.get_loaded_files();
    
    if (files.empty()) {
        std::cout << "  (None)" << std::endl;
        return true;
    }
    
    for (const auto& entry : files) {
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
            std::cout << "- " << entry.first << " (empty)\n";
        }
    }
    
    return true;
}

bool handleStats(const std::vector<std::string>& tokens, CsvServiceImpl& service) {
    if (!ServerMenu::checkArgCount(tokens, 0, 1)) {
        return true;
    }
    
    // Acquire shared lock for reading
    std::shared_lock<std::shared_mutex> lock(service.files_mutex);
    
    // Use get_loaded_files() method for backward compatibility
    const auto& files = service.get_loaded_files();
    
    if (files.empty()) {
        std::cout << "No files loaded." << std::endl;
        return true;
    }
    
    // If a filename is specified, show stats for that file only
    if (tokens.size() > 1) {
        const std::string& filename = tokens[1];
        auto it = files.find(filename);
        if (it == files.end()) {
            std::cerr << "Error: File '" << filename << "' not found." << std::endl;
            return true;
        }
        
        const auto& column_store = it->second;
        std::cout << "\nFile: " << filename << std::endl;
        
        // Basic stats
        std::cout << "  Columns: " << column_store.column_names.size() << std::endl;
        
        if (!column_store.column_names.empty()) {
            // Assuming all columns have the same length
            const auto& first_col = column_store.column_names[0];
            auto col_it = column_store.columns.find(first_col);
            size_t row_count = (col_it != column_store.columns.end()) ? col_it->second.size() : 0;
            std::cout << "  Rows: " << row_count << std::endl;
            
            // Column details
            std::cout << "  Column details:" << std::endl;
            for (const auto& col_name : column_store.column_names) {
                col_it = column_store.columns.find(col_name);
                if (col_it != column_store.columns.end()) {
                    std::cout << "    - " << col_name << " (" << col_it->second.size() << " values)" << std::endl;
                }
            }
        }
    } else {
        // Show summary stats for all files
        std::cout << "\nOverall Statistics:" << std::endl;
        std::cout << "  Total files loaded: " << files.size() << std::endl;
        
        size_t total_columns = 0;
        size_t total_rows = 0;
        
        for (const auto& [filename, column_store] : files) {
            total_columns += column_store.column_names.size();
            
            // Assuming all columns have the same number of rows, use the first column
            if (!column_store.column_names.empty()) {
                const auto& col_name = column_store.column_names[0];
                auto col_it = column_store.columns.find(col_name);
                if (col_it != column_store.columns.end()) {
                    total_rows += col_it->second.size();
                }
            }
        }
        
        std::cout << "  Total columns across all files: " << total_columns << std::endl;
        std::cout << "  Total rows across all files: " << total_rows << std::endl;
    }
    
    return true;
}

bool handleIpAddress(const std::vector<std::string>& tokens, CsvServiceImpl& service) {
    if (!ServerMenu::checkArgCount(tokens, 0, 0)) {
        return true;
    }
    
    std::cout << "\nServer Network Information:\n";
    
    // Run the hostname command to get the machine name
    std::array<char, 128> buffer;
    std::string hostname_result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen("hostname", "r"), pclose);
    if (!pipe) {
        std::cerr << "Error executing hostname command." << std::endl;
    } else {
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            hostname_result += buffer.data();
        }
        // Remove trailing newline
        if (!hostname_result.empty() && hostname_result[hostname_result.length()-1] == '\n') {
            hostname_result.erase(hostname_result.length()-1);
        }
        std::cout << "  Hostname: " << hostname_result << std::endl;
    }
    
    // Run the ifconfig/ip command to get IP addresses
    std::string ip_result;
    #ifdef _WIN32
    std::unique_ptr<FILE, decltype(&pclose)> ip_pipe(popen("ipconfig", "r"), pclose);
    #else
    std::unique_ptr<FILE, decltype(&pclose)> ip_pipe(popen("ifconfig 2>/dev/null || ip addr", "r"), pclose);
    #endif
    
    if (!ip_pipe) {
        std::cerr << "Error executing network command." << std::endl;
    } else {
        while (fgets(buffer.data(), buffer.size(), ip_pipe.get()) != nullptr) {
            ip_result += buffer.data();
        }
        
        std::cout << "\n  Network Interfaces:\n" << ip_result << std::endl;
    }
    
    std::cout << "\n  Server is listening on port 50051\n";
    std::cout << "  Clients can connect using: <IP_ADDRESS>:50051\n";
    std::cout << "  For local connections use: localhost:50051\n";
    
    return true;
}

bool handleExit(const std::vector<std::string>& tokens, CsvServiceImpl& service) {
    if (!ServerMenu::checkArgCount(tokens, 0, 0)) {
        return true;
    }
    
    std::cout << "Shutting down server..." << std::endl;
    return false; // Signal to exit the command loop
}

bool handleHelp(const std::vector<std::string>& tokens, CsvServiceImpl& service) {
    std::cout << "Available server commands:" << std::endl;
    std::cout << "  list               - List all loaded files with basic info" << std::endl;
    std::cout << "  stats [filename]   - Show statistics for all files or a specific file" << std::endl;
    std::cout << "  ip                 - Display server IP address information for client connections" << std::endl;
    std::cout << "  exit               - Shutdown the server" << std::endl;
    std::cout << "  help               - Display this help message" << std::endl;
    return true;
}

} // namespace commands

std::unique_ptr<ServerMenu> createServerMenu() {
    auto menu = std::make_unique<ServerMenu>();
    
    // Register commands with their handlers
    menu->registerCommand("list", commands::handleList, "List all loaded files");
    menu->registerCommand("stats", commands::handleStats, "Show statistics for files");
    menu->registerCommand("ip", commands::handleIpAddress, "Display IP address information");
    menu->registerCommand("exit", commands::handleExit, "Shutdown server");
    menu->registerCommand("help", commands::handleHelp, "Display help information");
    
    return menu;
}

} // namespace server
