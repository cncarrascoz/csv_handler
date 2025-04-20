#pragma once

#include <string>
#include <vector>
#include <unordered_map>

/**
 * TableView provides a read-only view of tabular data
 * This is a non-mutable interface used to access table data
 * without exposing the underlying storage details
 */
class TableView {
public:
    std::vector<std::string> column_names;
    std::unordered_map<std::string, std::vector<std::string>> columns;
    
    TableView() = default;
    
    /**
     * Construct a TableView from column names and data
     */
    TableView(const std::vector<std::string>& col_names,
              const std::unordered_map<std::string, std::vector<std::string>>& col_data)
        : column_names(col_names), columns(col_data) {}
    
    /**
     * Check if the view has data
     * @return true if the view has columns and data
     */
    bool empty() const {
        return column_names.empty() || columns.empty();
    }
    
    /**
     * Get the number of rows in the view
     * @return Number of rows, or 0 if the view is empty
     */
    size_t row_count() const {
        if (column_names.empty() || columns.empty()) {
            return 0;
        }
        
        // Find the first column and return its size
        auto it = columns.find(column_names[0]);
        if (it != columns.end()) {
            return it->second.size();
        }
        return 0;
    }
    
    /**
     * Utility method to compute the sum of a numeric column
     * @param column_name Name of the column to sum
     * @return Sum of all numeric values in the column
     * @throws std::runtime_error if column not found or no valid numeric values
     */
    double compute_sum(const std::string& column_name) const;
    
    /**
     * Utility method to compute the average of a numeric column
     * @param column_name Name of the column to average
     * @return Average of all numeric values in the column
     * @throws std::runtime_error if column not found or no valid numeric values
     */
    double compute_average(const std::string& column_name) const;
};
