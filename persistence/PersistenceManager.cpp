#include "PersistenceManager.hpp"
#include <iostream>
#include <filesystem>

PersistenceManager::PersistenceManager(
    std::shared_ptr<InMemoryStateMachine> mem_state,
    const std::string& data_dir,
    const std::string& log_dir)
    : mem_state_(mem_state),
      data_dir_(data_dir),
      log_dir_(log_dir) {
    
    // Create the durable state machine
    durable_state_ = std::make_unique<DurableStateMachine>(data_dir, log_dir);
    
    std::cout << "PersistenceManager initialized with data_dir=" << data_dir
              << " and log_dir=" << log_dir << std::endl;
}

PersistenceManager::~PersistenceManager() {
    // Create a final snapshot before shutting down
    create_snapshot();
}

bool PersistenceManager::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "Initializing PersistenceManager..." << std::endl;
    
    // Recover state from disk
    bool recovery_success = durable_state_->recover();
    
    if (recovery_success) {
        std::cout << "Recovery successful" << std::endl;
        
        // Copy state from durable state machine to in-memory state machine
        const auto& files = durable_state_->list_files();
        for (const auto& file : files) {
            // Get the file data from the durable state machine
            TableView view = durable_state_->view(file);
            
            // Add the file to the in-memory state machine
            // Use the direct member access for column_names and columns
            mem_state_->add_csv_file(file, view.column_names, view.columns);
        }
        
        // Update the last applied index
        // Since DurableStateMachine doesn't expose last_applied_index directly,
        // we'll just set our internal counter to 0 and increment it with each mutation
        last_applied_index_ = 0;
    } else {
        std::cerr << "Recovery failed, starting with empty state" << std::endl;
    }
    
    return recovery_success;
}

void PersistenceManager::observe_mutation(const Mutation& mutation) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Apply the mutation to the durable state machine
    durable_state_->apply(mutation);
    
    // Increment our internal counter
    last_applied_index_++;
}

bool PersistenceManager::create_snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Create a snapshot using the durable state machine
    return durable_state_->create_snapshot();
}

uint64_t PersistenceManager::last_applied_index() const {
    return last_applied_index_;
}
