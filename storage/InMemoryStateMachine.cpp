#include "InMemoryStateMachine.hpp"
#include <stdexcept>
#include <algorithm>

void InMemoryStateMachine::apply(const Mutation& mutation) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // Get the file name from the mutation
    const std::string& filename = mutation.file;
    
    // Ensure the file exists
    if (files_.find(filename) == files_.end()) {
        throw std::runtime_error("File not found: " + filename);
    }
    
    auto& store = files_[filename];
    
    // Apply the appropriate mutation
    if (mutation.has_insert()) {
        insert_row(store, mutation.insert().values);
    } else if (mutation.has_delete()) {
        delete_row(store, mutation.del().row_index);
    } else {
        throw std::runtime_error("Unknown mutation type");
    }
}

TableView InMemoryStateMachine::view(const std::string& file) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = files_.find(file);
    if (it == files_.end()) {
        return TableView(); // Return empty view if file not found
    }
    
    const auto& store = it->second;
    return TableView(store.column_names, store.columns);
}

std::vector<std::string> InMemoryStateMachine::list_files() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<std::string> filenames;
    for (const auto& entry : files_) {
        filenames.push_back(entry.first);
    }
    return filenames;
}

bool InMemoryStateMachine::file_exists(const std::string& file) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return files_.find(file) != files_.end();
}

void InMemoryStateMachine::add_csv_file(
    const std::string& filename,
    const std::vector<std::string>& column_names,
    const std::unordered_map<std::string, std::vector<std::string>>& columns) {
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    ColumnStore store;
    store.column_names = column_names;
    store.columns = columns;
    
    files_[filename] = std::move(store);
}

// Helper methods for mutations
void InMemoryStateMachine::insert_row(ColumnStore& store, const std::vector<std::string>& values) {
    // Check if the number of values matches the number of columns
    if (values.size() != store.column_names.size()) {
        throw std::runtime_error("Number of values does not match the number of columns");
    }
    
    // Add each value to its corresponding column
    for (size_t i = 0; i < store.column_names.size(); ++i) {
        const std::string& column_name = store.column_names[i];
        store.columns[column_name].push_back(values[i]);
    }
}

void InMemoryStateMachine::delete_row(ColumnStore& store, int row_index) {
    // Check if the row index is valid
    if (store.column_names.empty()) {
        throw std::runtime_error("No columns in store");
    }
    
    const auto& first_column = store.columns[store.column_names[0]];
    if (row_index < 0 || row_index >= static_cast<int>(first_column.size())) {
        throw std::runtime_error("Row index out of bounds");
    }
    
    // Delete the value at the specified index from each column
    for (const auto& column_name : store.column_names) {
        auto& values = store.columns[column_name];
        values.erase(values.begin() + row_index);
    }
}
