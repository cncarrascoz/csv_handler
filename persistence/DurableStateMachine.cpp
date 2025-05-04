#include "DurableStateMachine.hpp"
#include <iostream>
#include <filesystem>

DurableStateMachine::DurableStateMachine(
    const std::string& data_dir,
    const std::string& log_dir)
    : data_dir_(data_dir),
      mem_state_(std::make_unique<InMemoryStateMachine>()),
      wal_(std::make_unique<WriteAheadLog>(log_dir)) {
    
    // Create directories if they don't exist
    std::filesystem::create_directories(data_dir);
    std::filesystem::create_directories(log_dir);
    
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
    
    // Check if we should create a snapshot
    if (log_index % 100 == 0) { // Create a snapshot every 100 mutations
        create_snapshot();
    }
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
    
    std::cout << "Creating snapshot at index " << last_applied_index_ << std::endl;
    
    // Create a snapshot directory with timestamp
    std::string snapshot_dir = data_dir_ + "/snapshot_" + std::to_string(last_applied_index_);
    
    // Get all files from the in-memory state
    std::unordered_map<std::string, Snapshot::FileData> snapshot_files;
    const auto& mem_files = mem_state_->get_files();
    
    for (const auto& [filename, column_store] : mem_files) {
        Snapshot::FileData file_data;
        file_data.column_names = column_store.column_names;
        file_data.columns = column_store.columns;
        snapshot_files[filename] = file_data;
    }
    
    // Create the snapshot
    Snapshot snapshot(snapshot_files, last_applied_index_);
    
    // Save the snapshot to disk
    bool success = snapshot.save(snapshot_dir);
    
    // If successful, truncate the WAL
    if (success && last_applied_index_ > 0) {
        wal_->truncate(last_applied_index_);
    }
    
    return success;
}

bool DurableStateMachine::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "Starting recovery process..." << std::endl;
    
    // Find the latest snapshot
    std::string latest_snapshot_dir;
    uint64_t latest_snapshot_index = 0;
    
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        if (entry.is_directory()) {
            std::string dir_name = entry.path().filename().string();
            if (dir_name.find("snapshot_") == 0) {
                try {
                    // Extract the index from the directory name, handling potential parsing errors
                    std::string index_str = dir_name.substr(9);
                    if (!index_str.empty()) {
                        uint64_t index = std::stoull(index_str);
                        if (index > latest_snapshot_index) {
                            latest_snapshot_index = index;
                            latest_snapshot_dir = entry.path().string();
                        }
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing snapshot directory name: " << dir_name 
                              << " - " << e.what() << std::endl;
                    // Continue with the next directory
                }
            }
        }
    }
    
    // Load the latest snapshot if found
    if (!latest_snapshot_dir.empty()) {
        std::cout << "Loading snapshot from: " << latest_snapshot_dir << std::endl;
        try {
            Snapshot snapshot = Snapshot::load(latest_snapshot_dir);
            
            // Apply the snapshot to the in-memory state
            for (const auto& filename : snapshot.file_list()) {
                Snapshot::FileData file_data = snapshot.get_file_data(filename);
                mem_state_->add_csv_file(filename, file_data.column_names, file_data.columns);
            }
            
            last_applied_index_ = snapshot.last_included_index();
            std::cout << "Loaded snapshot with index: " << last_applied_index_ << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error loading snapshot from " << latest_snapshot_dir 
                      << " - " << e.what() << std::endl;
            std::cout << "Starting with empty state due to snapshot loading error" << std::endl;
            last_applied_index_ = 0;
        }
    } else {
        std::cout << "No snapshot found, starting with empty state" << std::endl;
        last_applied_index_ = 0;
    }
    
    // Apply any mutations from the WAL after the snapshot
    std::vector<Mutation> mutations = wal_->get_since(last_applied_index_);
    std::cout << "Applying " << mutations.size() << " mutations from WAL" << std::endl;
    
    for (const auto& mutation : mutations) {
        try {
            apply_internal(mutation);
            last_applied_index_++;
        } catch (const std::exception& e) {
            std::cerr << "Error applying mutation from WAL: " << e.what() << std::endl;
            // Continue with the next mutation
        }
    }
    
    std::cout << "Recovery complete, last applied index: " << last_applied_index_ << std::endl;
    return true;
}

void DurableStateMachine::apply_internal(const Mutation& mutation) {
    try {
        // Apply the mutation directly to the in-memory state without logging
        mem_state_->apply(mutation);
    } catch (const std::runtime_error& e) {
        // If the file doesn't exist and we're trying to insert, create it
        if (mutation.has_insert() && std::string(e.what()).find("File not found") != std::string::npos) {
            std::cout << "File " << mutation.file << " doesn't exist, creating it first" << std::endl;
            
            // Create an empty column store with column names derived from the insert values
            ColumnStore column_store;
            
            // For simplicity, we'll create generic column names (col0, col1, etc.)
            size_t num_columns = mutation.insert().values.size();
            for (size_t i = 0; i < num_columns; ++i) {
                std::string col_name = "col" + std::to_string(i);
                column_store.column_names.push_back(col_name);
                column_store.columns[col_name] = std::vector<std::string>();
            }
            
            // Add the empty file to the state machine
            mem_state_->add_csv_file(mutation.file, column_store.column_names, column_store.columns);
            
            // Now try applying the mutation again
            mem_state_->apply(mutation);
        } else {
            // For other errors, rethrow
            throw;
        }
    }
}
