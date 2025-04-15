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
