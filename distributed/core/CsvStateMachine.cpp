#include "CsvStateMachine.hpp"
#include "Mutation.hpp"
#include "../../storage/csv_parser.hpp"
#include <sstream>

namespace raft {

CsvStateMachine::CsvStateMachine() {
    // Initialize an empty state machine
}

bool CsvStateMachine::apply(const Mutation& mutation) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    switch (mutation.type()) {
        case MutationType::UPLOAD_CSV: {
            // Parse the CSV data
            ColumnStore column_store = csv::parse_csv(mutation.data());
            
            // Store the parsed data
            files_[mutation.filename()] = column_store;
            return true;
        }
        
        case MutationType::INSERT_ROW: {
            // Find the file
            auto it = files_.find(mutation.filename());
            if (it == files_.end()) {
                return false; // File not found
            }
            
            // Parse the row data (comma-separated values)
            std::vector<std::string> values;
            std::istringstream iss(mutation.data());
            std::string value;
            while (std::getline(iss, value, ',')) {
                values.push_back(value);
            }
            
            // Insert the values into each column
            auto& column_store = it->second;
            if (values.size() != column_store.column_names.size()) {
                return false; // Number of values doesn't match number of columns
            }
            
            for (size_t i = 0; i < column_store.column_names.size(); ++i) {
                const auto& column_name = column_store.column_names[i];
                column_store.columns[column_name].push_back(values[i]);
            }
            
            return true;
        }
        
        case MutationType::DELETE_ROW: {
            // Find the file
            auto it = files_.find(mutation.filename());
            if (it == files_.end()) {
                return false; // File not found
            }
            
            // Parse the row index
            int row_index;
            try {
                row_index = std::stoi(mutation.data());
            } catch (const std::exception& e) {
                return false; // Invalid row index
            }
            
            // Delete the row from each column
            auto& column_store = it->second;
            for (const auto& column_name : column_store.column_names) {
                auto& column = column_store.columns[column_name];
                if (row_index < 0 || row_index >= static_cast<int>(column.size())) {
                    return false; // Row index out of bounds
                }
                column.erase(column.begin() + row_index);
            }
            
            return true;
        }
        
        default:
            return false; // Unknown mutation type
    }
}

std::string CsvStateMachine::create_snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Serialize the state to a string
    std::ostringstream oss;
    
    // Write the number of files
    oss << files_.size() << "\n";
    
    // Write each file
    for (const auto& file_entry : files_) {
        const auto& filename = file_entry.first;
        const auto& column_store = file_entry.second;
        
        // Write the filename
        oss << filename << "\n";
        
        // Write the number of columns
        oss << column_store.column_names.size() << "\n";
        
        // Write each column name
        for (const auto& column_name : column_store.column_names) {
            oss << column_name << "\n";
        }
        
        // Write the number of rows
        size_t num_rows = column_store.columns.empty() ? 0 : 
                        column_store.columns.begin()->second.size();
        oss << num_rows << "\n";
        
        // Write each row
        for (size_t i = 0; i < num_rows; ++i) {
            for (size_t j = 0; j < column_store.column_names.size(); ++j) {
                if (j > 0) oss << ",";
                const auto& column_name = column_store.column_names[j];
                const auto& column = column_store.columns.at(column_name);
                oss << column[i];
            }
            oss << "\n";
        }
    }
    
    return oss.str();
}

bool CsvStateMachine::restore_snapshot(const std::string& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Clear the current state
    files_.clear();
    
    // Parse the snapshot
    std::istringstream iss(snapshot);
    std::string line;
    
    // Read the number of files
    if (!std::getline(iss, line)) return false;
    int num_files;
    try {
        num_files = std::stoi(line);
    } catch (const std::exception& e) {
        return false;
    }
    
    // Read each file
    for (int i = 0; i < num_files; ++i) {
        // Read the filename
        if (!std::getline(iss, line)) return false;
        std::string filename = line;
        
        // Read the number of columns
        if (!std::getline(iss, line)) return false;
        int num_columns;
        try {
            num_columns = std::stoi(line);
        } catch (const std::exception& e) {
            return false;
        }
        
        // Read each column name
        std::vector<std::string> column_names;
        for (int j = 0; j < num_columns; ++j) {
            if (!std::getline(iss, line)) return false;
            column_names.push_back(line);
        }
        
        // Read the number of rows
        if (!std::getline(iss, line)) return false;
        int num_rows;
        try {
            num_rows = std::stoi(line);
        } catch (const std::exception& e) {
            return false;
        }
        
        // Initialize the column store
        ColumnStore column_store;
        column_store.column_names = column_names;
        
        // Read each row
        for (int j = 0; j < num_rows; ++j) {
            if (!std::getline(iss, line)) return false;
            
            // Parse the row
            std::vector<std::string> values;
            std::istringstream row_iss(line);
            std::string value;
            while (std::getline(row_iss, value, ',')) {
                values.push_back(value);
            }
            
            // Add the values to each column
            for (size_t k = 0; k < column_names.size(); ++k) {
                const auto& column_name = column_names[k];
                if (k < values.size()) {
                    column_store.columns[column_name].push_back(values[k]);
                } else {
                    column_store.columns[column_name].push_back("");
                }
            }
        }
        
        // Store the column store
        files_[filename] = column_store;
    }
    
    return true;
}

std::vector<std::string> CsvStateMachine::list_files() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::string> filenames;
    for (const auto& file_entry : files_) {
        filenames.push_back(file_entry.first);
    }
    
    return filenames;
}

std::pair<std::vector<std::string>, std::unordered_map<std::string, std::vector<std::string>>> 
CsvStateMachine::get_file(const std::string& filename) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_.find(filename);
    if (it == files_.end()) {
        // File not found
        return {{}, {}};
    }
    
    const auto& column_store = it->second;
    return {column_store.column_names, column_store.columns};
}

} // namespace raft
