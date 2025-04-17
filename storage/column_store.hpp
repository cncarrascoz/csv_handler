// Column-oriented storage for CSV data
#pragma once

#include <string>
#include <vector>
#include <unordered_map>

// Column-oriented storage for CSV data
struct ColumnStore {
    std::vector<std::string> column_names;
    std::unordered_map<std::string, std::vector<std::string>> columns;
};

// Computes the sum of a numeric column in the store
double compute_sum(const ColumnStore& store, const std::string& column_name);

// Computes the average of a numeric column in the store
double compute_average(const ColumnStore& store, const std::string& column_name);

// Insert a row into the store
void insert_row(ColumnStore& store, const std::vector<std::string>& values);

// Delete a row from the store
void delete_row(ColumnStore& store, int row_index);
