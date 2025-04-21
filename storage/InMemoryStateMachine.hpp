#pragma once

#include "core/IStateMachine.hpp"
#include "core/TableView.hpp"
#include "core/Mutation.hpp"

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <shared_mutex>

/**
 * In-memory implementation of the state machine interface
 * This class manages CSV data in memory using column-oriented storage
 */
class InMemoryStateMachine : public IStateMachine {
public:
    /**
     * Constructor
     */
    InMemoryStateMachine() = default;
    
    /**
     * Apply a mutation to the state machine
     * @param mutation The mutation to apply
     */
    void apply(const Mutation& mutation) override;
    
    /**
     * Get a view of the data for a specific file
     * @param file The name of the file to view
     * @return A read-only view of the file's data
     */
    TableView view(const std::string& file) const override;
    
    /**
     * Get a list of all files in the state machine
     * @return A vector of filenames
     */
    std::vector<std::string> list_files() const;
    
    /**
     * Check if a file exists in the state machine
     * @param file The name of the file to check
     * @return true if the file exists, false otherwise
     */
    bool file_exists(const std::string& file) const;
    
    /**
     * Add a new CSV file to the state machine
     * @param filename The name of the file
     * @param column_names The names of the columns
     * @param columns The data for each column
     */
    void add_csv_file(const std::string& filename, 
                     const std::vector<std::string>& column_names,
                     const std::unordered_map<std::string, std::vector<std::string>>& columns);

private:
    // Thread-safe access to the state
    mutable std::shared_mutex mutex_;
    
    // Column-oriented storage for each file
    struct ColumnStore {
        std::vector<std::string> column_names;
        std::unordered_map<std::string, std::vector<std::string>> columns;
    };
    
    std::unordered_map<std::string, ColumnStore> files_;
    
    // Helper methods for mutations
    void insert_row(ColumnStore& store, const std::vector<std::string>& values);
    void delete_row(ColumnStore& store, int row_index);
};
