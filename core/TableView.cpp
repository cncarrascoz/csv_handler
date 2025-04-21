#include "TableView.hpp"
#include <stdexcept>
#include <algorithm>
#include <numeric>

double TableView::compute_sum(const std::string& column_name) const {
    // Find the column
    auto column_it = columns.find(column_name);
    if (column_it == columns.end()) {
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

double TableView::compute_average(const std::string& column_name) const {
    // Find the column
    auto column_it = columns.find(column_name);
    if (column_it == columns.end()) {
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
