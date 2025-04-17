// client/menu.cpp: Implementation of the client menu system
#include "menu.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>

namespace client {

ClientMenu::ClientMenu() {}

void ClientMenu::registerCommand(const std::string& command, CommandHandler handler, 
                               const std::string& description) {
    command_handlers_[command] = {handler, description};
}

bool ClientMenu::processCommand(const std::string& command_line, CsvClient& client) {
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
    
    return it->second.handler(tokens, client);
}

void ClientMenu::displayMenu() const {
    std::cout << "\nClient commands:\n";
    
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

std::vector<std::string> ClientMenu::tokenizeCommand(const std::string& command_line) {
    std::vector<std::string> tokens;
    std::istringstream iss(command_line);
    std::string token;
    
    while (iss >> token) {
        tokens.push_back(token);
    }
    
    return tokens;
}

bool ClientMenu::checkArgCount(const std::vector<std::string>& tokens, 
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

bool handleUpload(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 1, 1)) {
        return true;
    }
    
    client.UploadCsv(tokens[1]);
    return true;
}

bool handleList(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 0, 0)) {
        return true;
    }
    
    client.ListFiles();
    return true;
}

bool handleExit(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 0, 0)) {
        return true;
    }
    
    std::cout << "Exiting interactive mode." << std::endl;
    return false; // Signal to exit the command loop
}

bool handleView(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 1, 1)) {
        return true;
    }
    
    client.ViewFile(tokens[1]);
    return true;
}

bool handleSum(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 2, 2)) {
        return true;
    }
    
    client.ComputeSum(tokens[1], tokens[2]);
    return true;
}

bool handleAvg(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 2, 2)) {
        return true;
    }
    
    client.ComputeAverage(tokens[1], tokens[2]);
    return true;
}

bool handleInsert(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 2, 100)) { // Allow many values
        return true;
    }
    
    // Reconstruct the comma-separated values from all tokens from index 2 onward
    std::string values;
    for (size_t i = 2; i < tokens.size(); ++i) {
        if (i > 2) values += ",";
        values += tokens[i];
    }
    
    client.InsertRow(tokens[1], values);
    return true;
}

bool handleDelete(const std::vector<std::string>& tokens, CsvClient& client) {
    if (!ClientMenu::checkArgCount(tokens, 2, 2)) {
        return true;
    }
    
    int row_index;
    try {
        row_index = std::stoi(tokens[2]);
    } catch (const std::exception& e) {
        std::cerr << "Error: Row index must be a valid integer." << std::endl;
        return true;
    }
    
    client.DeleteRow(tokens[1], row_index);
    return true;
}

bool handleHelp(const std::vector<std::string>& tokens, CsvClient& client) {
    std::cout << "Available commands:" << std::endl;
    std::cout << "  upload <filename>               - Upload a CSV file to the server" << std::endl;
    std::cout << "  list                            - List all files on the server" << std::endl;
    std::cout << "  view <filename>                 - View the contents of a file" << std::endl;
    std::cout << "  sum <filename> <column_name>    - Calculate sum of values in a column" << std::endl;
    std::cout << "  avg <filename> <column_name>    - Calculate average of values in a column" << std::endl;
    std::cout << "  insert <filename> <val1> <val2> - Insert a new row into a file" << std::endl;
    std::cout << "  delete <filename> <row_index>   - Delete a row from a file" << std::endl;
    std::cout << "  exit                            - Exit the program" << std::endl;
    std::cout << "  help                            - Display this help message" << std::endl;
    return true;
}

} // namespace commands

std::unique_ptr<ClientMenu> createClientMenu() {
    auto menu = std::make_unique<ClientMenu>();
    
    // Register all command handlers
    menu->registerCommand("upload", commands::handleUpload,
                        "Upload a CSV file to the server");
    menu->registerCommand("list", commands::handleList,
                        "List all files on the server");
    menu->registerCommand("exit", commands::handleExit,
                        "Exit the program");
    menu->registerCommand("view", commands::handleView,
                        "View the contents of a file");
    menu->registerCommand("sum", commands::handleSum,
                        "Calculate sum of values in a column");
    menu->registerCommand("avg", commands::handleAvg,
                        "Calculate average of values in a column");
    menu->registerCommand("insert", commands::handleInsert,
                        "Insert a new row into a file");
    menu->registerCommand("delete", commands::handleDelete,
                        "Delete a row from a file");
    menu->registerCommand("help", commands::handleHelp,
                        "Display help information");
    
    return menu;
}

} // namespace client
