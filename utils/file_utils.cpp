#include "file_utils.hpp"
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <vector>

namespace file_utils {

std::string read_file(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }
    
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::string contents(size, '\0');
    if (!file.read(&contents[0], size)) {
        throw std::runtime_error("Failed to read file: " + filename);
    }
    
    return contents;
}

std::string format_csv_as_table(const std::vector<std::string>& column_names, 
                               const std::vector<std::vector<std::string>>& rows) {
    if (column_names.empty()) {
        return "Empty table";
    }

    // Calculate the width needed for each column
    std::vector<size_t> column_widths(column_names.size(), 0);
    
    // Consider column names for width calculation
    for (size_t i = 0; i < column_names.size(); ++i) {
        column_widths[i] = std::max(column_widths[i], column_names[i].length());
    }
    
    // Consider data for width calculation
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size() && i < column_widths.size(); ++i) {
            column_widths[i] = std::max(column_widths[i], row[i].length());
        }
    }
    
    // Add padding (2 spaces on each side)
    for (auto& width : column_widths) {
        width += 4;  // 2 spaces on each side
    }
    
    std::ostringstream oss;
    
    // Helper function to draw a horizontal line
    auto draw_horizontal_line = [&column_widths, &oss](char left, char middle, char right, char fill) {
        oss << left << "\n";
        for (size_t i = 0; i < column_widths.size(); ++i) {
            oss << std::string(column_widths[i], fill);
            if (i < column_widths.size() - 1) {
                oss << middle;
            }
        }
        oss << right << "\n";
    };
    
    // Top border
    draw_horizontal_line('+', '+', '+', '-');
    
    // Header row
    oss << "|";
    for (size_t i = 0; i < column_names.size(); ++i) {
        oss << "  " << std::setw(column_widths[i] - 4) << std::left << column_names[i] << "  ";
        if (i < column_names.size() - 1) {
            oss << "|";
        }
    }
    oss << "|\n";
    
    // Header-data separator
    draw_horizontal_line('+', '+', '+', '-');
    
    // Data rows
    for (const auto& row : rows) {
        oss << "|";
        for (size_t i = 0; i < column_names.size(); ++i) {
            std::string value = (i < row.size()) ? row[i] : "";
            oss << "  " << std::setw(column_widths[i] - 4) << std::left << value << "  ";
            if (i < column_names.size() - 1) {
                oss << "|";
            }
        }
        oss << "|\n";
    }
    
    // Bottom border
    draw_horizontal_line('+', '+', '+', '-');
    
    return oss.str();
}

} // namespace file_utils
