// File utility functions
#pragma once

#include <string>
#include <fstream>

namespace file_utils {

/**
 * Reads entire file contents into a string
 * @param filename Path to file
 * @return File contents as string
 * @throws std::runtime_error if file cannot be read
 */
std::string read_file(const std::string& filename);

} // namespace file_utils
