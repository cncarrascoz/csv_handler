#include "file_utils.hpp"
#include <stdexcept>

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

} // namespace file_utils
