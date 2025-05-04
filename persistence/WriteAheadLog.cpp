#include "WriteAheadLog.hpp"
#include <filesystem>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <sstream>

WriteAheadLog::WriteAheadLog(const std::string& log_dir) : log_dir_(log_dir) {
    // Create the log directory if it doesn't exist
    std::filesystem::create_directories(log_dir);
    
    // Scan existing log files and recover state
    std::string log_file_path = log_dir + "/wal.log";
    
    // Check if log file exists
    if (std::filesystem::exists(log_file_path)) {
        std::cout << "Found existing WAL file: " << log_file_path << std::endl;
        
        // Open the log file for reading
        std::ifstream log_file(log_file_path, std::ios::binary);
        if (!log_file.is_open()) {
            std::cerr << "Failed to open WAL file for reading: " << log_file_path << std::endl;
            return;
        }
        
        // Read and deserialize mutations
        std::string line;
        while (std::getline(log_file, line)) {
            try {
                Mutation mutation = deserialize_mutation(line);
                mutations_.push_back(mutation);
                last_index_++;
            } catch (const std::exception& e) {
                std::cerr << "Error deserializing mutation: " << e.what() << std::endl;
                // Continue with next line
            }
        }
        
        std::cout << "Recovered " << mutations_.size() << " mutations from WAL" << std::endl;
    } else {
        std::cout << "No existing WAL file found, creating new one at: " << log_file_path << std::endl;
    }
}

uint64_t WriteAheadLog::append(const Mutation& mutation) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Serialize the mutation
    std::string serialized = serialize_mutation(mutation);
    
    // Append to log file
    std::string log_file_path = log_dir_ + "/wal.log";
    std::ofstream log_file(log_file_path, std::ios::app);
    if (!log_file.is_open()) {
        throw std::runtime_error("Failed to open WAL file for writing: " + log_file_path);
    }
    
    // Write the serialized mutation
    log_file << serialized << std::endl;
    log_file.flush(); // Ensure it's written to disk
    
    // Store in memory and increment the index
    mutations_.push_back(mutation);
    last_index_++;
    
    return last_index_;
}

std::vector<Mutation> WriteAheadLog::get_since(uint64_t since_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (since_index >= last_index_) {
        return {};
    }
    
    size_t start_pos = since_index; // Assuming 0-based indexing for simplicity
    if (start_pos >= mutations_.size()) {
        return {};
    }
    
    std::vector<Mutation> result;
    for (size_t i = start_pos; i < mutations_.size(); ++i) {
        result.push_back(mutations_[i]);
    }
    
    return result;
}

uint64_t WriteAheadLog::last_index() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_index_;
}

void WriteAheadLog::truncate(uint64_t up_to_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (up_to_index > 0 && up_to_index <= last_index_) {
        // Truncate the in-memory vector
        size_t keep_from = std::min(static_cast<size_t>(up_to_index), mutations_.size());
        mutations_.erase(mutations_.begin(), mutations_.begin() + keep_from);
        
        // Rewrite the log file with remaining mutations
        std::string log_file_path = log_dir_ + "/wal.log";
        std::ofstream log_file(log_file_path, std::ios::trunc);
        if (!log_file.is_open()) {
            std::cerr << "Failed to open WAL file for truncation: " << log_file_path << std::endl;
            return;
        }
        
        for (const auto& mutation : mutations_) {
            log_file << serialize_mutation(mutation) << std::endl;
        }
        log_file.flush();
    }
}

// Helper method to serialize a mutation to a string
std::string WriteAheadLog::serialize_mutation(const Mutation& mutation) const {
    std::stringstream ss;
    
    // Format: file|operation_type|data
    // For RowInsert: file|insert|val1,val2,val3
    // For RowDelete: file|delete|row_index
    
    ss << mutation.file << "|";
    
    if (mutation.has_insert()) {
        ss << "insert|";
        const auto& values = mutation.insert().values;
        for (size_t i = 0; i < values.size(); ++i) {
            if (i > 0) ss << ",";
            ss << values[i];
        }
    } else if (mutation.has_delete()) {
        ss << "delete|" << mutation.del().row_index;
    }
    
    return ss.str();
}

// Helper method to deserialize a mutation from a string
Mutation WriteAheadLog::deserialize_mutation(const std::string& serialized) const {
    std::stringstream ss(serialized);
    std::string file, op_type, data;
    
    // Parse file
    if (!std::getline(ss, file, '|')) {
        throw std::runtime_error("Invalid mutation format: missing file");
    }
    
    // Parse operation type
    if (!std::getline(ss, op_type, '|')) {
        throw std::runtime_error("Invalid mutation format: missing operation type");
    }
    
    // Parse data
    if (!std::getline(ss, data)) {
        throw std::runtime_error("Invalid mutation format: missing data");
    }
    
    Mutation mutation;
    mutation.file = file;
    
    if (op_type == "insert") {
        // Parse comma-separated values
        std::vector<std::string> values;
        std::stringstream data_ss(data);
        std::string value;
        while (std::getline(data_ss, value, ',')) {
            values.push_back(value);
        }
        
        RowInsert insert;
        insert.values = values;
        mutation.op = insert;
    } else if (op_type == "delete") {
        // Parse row index
        int row_index = std::stoi(data);
        
        RowDelete del;
        del.row_index = row_index;
        mutation.op = del;
    } else {
        throw std::runtime_error("Invalid operation type: " + op_type);
    }
    
    return mutation;
}
