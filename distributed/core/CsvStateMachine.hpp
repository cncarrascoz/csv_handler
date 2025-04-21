#pragma once

#include "RaftStateMachine.hpp"
#include <mutex>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include "../../storage/column_store.hpp"

namespace raft {

/**
 * Implementation of RaftStateMachine for the CSV handler system
 * This class implements a standalone CSV state machine for Raft consensus
 */
class CsvStateMachine : public RaftStateMachine {
public:
    /**
     * Default constructor
     */
    CsvStateMachine();
    
    /**
     * Apply a mutation to the state machine
     * @param mutation The mutation to apply
     * @return True if the mutation was applied successfully
     */
    bool apply(const Mutation& mutation) override;
    
    /**
     * Create a snapshot of the current state
     * @return A serialized representation of the current state
     */
    std::string create_snapshot() override;
    
    /**
     * Restore the state from a snapshot
     * @param snapshot The serialized state to restore from
     * @return True if the state was restored successfully
     */
    bool restore_snapshot(const std::string& snapshot) override;
    
    /**
     * List all files in the state machine
     * @return A list of filenames
     */
    std::vector<std::string> list_files() const;
    
    /**
     * Get a file from the state machine
     * @param filename The name of the file to get
     * @return A pair containing the column names and column data
     */
    std::pair<std::vector<std::string>, std::unordered_map<std::string, std::vector<std::string>>> 
    get_file(const std::string& filename) const;

private:
    // Mutex for thread-safe access to the state
    mutable std::mutex mutex_;
    
    // Map of filename to column store
    std::unordered_map<std::string, ColumnStore> files_;
};

} // namespace raft
