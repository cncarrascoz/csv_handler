#pragma once

#include "core/IStateMachine.hpp"
#include "storage/InMemoryStateMachine.hpp"
#include "WriteAheadLog.hpp"
#include "Snapshot.hpp"

#include <memory>
#include <string>
#include <mutex>

/**
 * DurableStateMachine wraps an InMemoryStateMachine and provides durability
 * by logging mutations to a WriteAheadLog before applying them
 */
class DurableStateMachine : public IStateMachine {
public:
    /**
     * Constructor
     * @param data_dir Directory where data files will be stored
     * @param log_dir Directory where WAL files will be stored
     */
    explicit DurableStateMachine(
        const std::string& data_dir = "data",
        const std::string& log_dir = "logs");
    
    /**
     * Apply a mutation to the state machine
     * First logs to WAL, then applies to in-memory state
     * @param mutation The mutation to apply
     */
    void apply(const Mutation& mutation) override;
    
    /**
     * Get a view of a specific file in the state machine
     * @param file The name of the file to view
     * @return A view of the file's data
     */
    TableView view(const std::string& file) const override;
    
    /**
     * Get a list of all files in the state machine
     * @return A vector of filenames
     */
    std::vector<std::string> list_files() const;
    
    /**
     * Create a snapshot of the current state
     * @return True if the snapshot was created successfully
     */
    bool create_snapshot();
    
    /**
     * Recover state from the latest snapshot and WAL
     * @return True if recovery was successful
     */
    bool recover();

private:
    std::string data_dir_;
    std::unique_ptr<InMemoryStateMachine> mem_state_;
    std::unique_ptr<WriteAheadLog> wal_;
    mutable std::mutex mutex_;
    uint64_t last_applied_index_ = 0;
    
    // Helper method to apply a mutation without logging
    void apply_internal(const Mutation& mutation);
};
