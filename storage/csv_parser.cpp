#include "csv_parser.hpp"

namespace csv {

ColumnStore parse_csv(const std::string& csv_data) {
    ColumnStore column_store;
    std::istringstream ss(csv_data);
    std::string line;
    
    // Read header (first line)
    if (std::getline(ss, line)) {
        std::istringstream header_stream(line);
        std::string column_name;
        
        while (std::getline(header_stream, column_name, ',')) {
            column_store.column_names.push_back(column_name);
        }
    }
    
    // Read data rows
    size_t row_count = 0;
    while (std::getline(ss, line)) {
        std::istringstream row_stream(line);
        std::string value;
        size_t col_index = 0;
        
        while (std::getline(row_stream, value, ',')) {
            if (col_index < column_store.column_names.size()) {
                std::string column_name = column_store.column_names[col_index];
                column_store.columns[column_name].push_back(value);
            }
            col_index++;
        }
        row_count++;
    }
    
    return column_store;
}

} // namespace csv
