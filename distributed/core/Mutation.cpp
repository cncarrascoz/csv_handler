#include "Mutation.hpp"
#include <sstream>

namespace raft {

std::string Mutation::serialize() const {
    std::stringstream ss;
    ss << static_cast<int>(type_) << "|"
       << filename_ << "|"
       << data_;
    return ss.str();
}

Mutation Mutation::deserialize(const std::string& serialized) {
    std::stringstream ss(serialized);
    std::string type_str, filename, data;
    
    // Parse the type
    std::getline(ss, type_str, '|');
    std::getline(ss, filename, '|');
    std::getline(ss, data);
    
    int type_int = std::stoi(type_str);
    MutationType type = static_cast<MutationType>(type_int);
    
    return Mutation(type, filename, data);
}

} // namespace raft
