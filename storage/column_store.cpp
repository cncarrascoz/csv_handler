// storage/column_store.cpp: Implementation of column store operations
#include "column_store.hpp"
#include <stdexcept>
#include <algorithm>
#include <numeric>

// Computes the sum of a numeric column in the store
double compute_sum(const ColumnStore& store, const std::string& column_name) {
    // Find the column
    auto column_it = store.columns.find(column_name);
    if (column_it == store.columns.end()) {
        throw std::runtime_error("Column not found");
    }
    
    const auto& values = column_it->second;
    double sum = 0.0;
    
    // Sum all values that can be converted to numbers
    for (const auto& value : values) {
        try {
            sum += std::stod(value);
        } catch (const std::exception& e) {
            // Skip non-numeric values
        }
    }
    
    return sum;
}

// Computes the average of a numeric column in the store
double compute_average(const ColumnStore& store, const std::string& column_name) {
    // Find the column
    auto column_it = store.columns.find(column_name);
    if (column_it == store.columns.end()) {
        throw std::runtime_error("Column not found");
    }
    
    const auto& values = column_it->second;
    double sum = 0.0;
    int count = 0;
    
    // Sum all values that can be converted to numbers
    for (const auto& value : values) {
        try {
            sum += std::stod(value);
            count++;
        } catch (const std::exception& e) {
            // Skip non-numeric values
        }
    }
    
    if (count == 0) {
        throw std::runtime_error("No numeric values found in column");
    }
    
    return sum / count;
}

// Insert a row into the store
void insert_row(ColumnStore& store, const std::vector<std::string>& values) {
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

// Delete a row from the store
void delete_row(ColumnStore& store, int row_index) {
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
