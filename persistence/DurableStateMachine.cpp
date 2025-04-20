#include "DurableStateMachine.hpp"
#include <iostream>
#include <filesystem>

DurableStateMachine::DurableStateMachine(
    const std::string& data_dir,
    const std::string& log_dir)
    : data_dir_(data_dir),
      mem_state_(std::make_unique<InMemoryStateMachine>()),
      wal_(std::make_unique<WriteAheadLog>(log_dir)) {
    
    // In a real implementation, we would:
    // 1. Create directories if they don't exist
    // 2. Recover state from snapshot and WAL
    
    // For now, just print a message
    std::cout << "DurableStateMachine initialized with data_dir=" << data_dir
              << " and log_dir=" << log_dir << std::endl;
}

void DurableStateMachine::apply(const Mutation& mutation) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // First log the mutation to the WAL
    uint64_t log_index = wal_->append(mutation);
    
    // Then apply the mutation to the in-memory state
    apply_internal(mutation);
    
    // Update the last applied index
    last_applied_index_ = log_index;
}

TableView DurableStateMachine::view(const std::string& file) const {
    // No need for extra locking here as the InMemoryStateMachine is already thread-safe
    return mem_state_->view(file);
}

std::vector<std::string> DurableStateMachine::list_files() const {
    return mem_state_->list_files();
}

bool DurableStateMachine::create_snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // In a real implementation, we would:
    // 1. Create a snapshot by serializing the current state
    // 2. Write it to a snapshot file
    // 3. Truncate the WAL up to last_applied_index_
    
    // For now, just print a message
    std::cout << "Creating snapshot at index " << last_applied_index_ << std::endl;
    
    // Truncate the WAL
    if (last_applied_index_ > 0) {
        wal_->truncate(last_applied_index_);
    }
    
    return true;
}

bool DurableStateMachine::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // In a real implementation, we would:
    // 1. Load the latest snapshot
    // 2. Apply all mutations from the WAL after the snapshot index
    
    // For now, just print a message
    std::cout << "DurableStateMachine::recover() called (not actually implemented)" << std::endl;
    
    return true;
}

void DurableStateMachine::apply_internal(const Mutation& mutation) {
    // Apply the mutation directly to the in-memory state without logging
    mem_state_->apply(mutation);
}
