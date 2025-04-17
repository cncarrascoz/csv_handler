// File utility functions
#pragma once

#include <string>
#include <fstream>
#include <vector>

namespace file_utils {

/**
 * Reads entire file contents into a string
 * @param filename Path to file
 * @return File contents as string
 * @throws std::runtime_error if file cannot be read
 */
std::string read_file(const std::string& filename);

/**
 * Formats CSV data as a table with plus signs (+) as corners and dashes (-) as horizontal borders
 * @param column_names Vector of column names
 * @param rows Vector of rows, where each row is a vector of string values
 * @return Formatted table as string
 */
std::string format_csv_as_table(const std::vector<std::string>& column_names, 
                               const std::vector<std::vector<std::string>>& rows);

} // namespace file_utils
