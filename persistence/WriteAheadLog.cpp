#include "WriteAheadLog.hpp"
#include <filesystem>
#include <stdexcept>
#include <iostream>

WriteAheadLog::WriteAheadLog(const std::string& log_dir) : log_dir_(log_dir) {
    // In a real implementation, we would:
    // 1. Create the log directory if it doesn't exist
    // 2. Scan existing log files and recover state
    // 3. Initialize the last_index_ based on existing logs
    
    // This is just a stub implementation
    std::cout << "WAL initialized with directory: " << log_dir << " (not actually used yet)" << std::endl;
}

uint64_t WriteAheadLog::append(const Mutation& mutation) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // In a real implementation, we would:
    // 1. Serialize the mutation to a binary format
    // 2. Append it to a log file
    // 3. Call fsync() to ensure durability
    
    // For now, just store in memory and increment the index
    mutations_.push_back(mutation);
    last_index_++;
    
    return last_index_;
}

std::vector<Mutation> WriteAheadLog::get_since(uint64_t since_index) const {
    // Use the mutable mutex directly in const methods
    std::lock_guard<std::mutex> lock(mutex_);
    
    // In a real implementation, we would:
    // 1. Find the log file containing the since_index
    // 2. Read and deserialize all mutations after that index
    
    // For now, just return a subset of the in-memory vector
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
    
    // In a real implementation, we would:
    // 1. Delete log files that only contain entries before up_to_index
    // 2. Update any in-memory bookkeeping
    
    // For now, just truncate the in-memory vector
    if (up_to_index > 0 && up_to_index <= last_index_) {
        size_t keep_from = std::min(static_cast<size_t>(up_to_index), mutations_.size());
        mutations_.erase(mutations_.begin(), mutations_.begin() + keep_from);
    }
}
