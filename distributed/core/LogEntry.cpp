#include "LogEntry.hpp"
#include <sstream>

namespace raft {

std::string LogEntry::serialize() const {
    std::stringstream ss;
    ss << term_ << "|"
       << index_ << "|"
       << mutation_.serialize();
    return ss.str();
}

LogEntry LogEntry::deserialize(const std::string& serialized) {
    std::stringstream ss(serialized);
    
    std::string term_str, index_str, mutation_str;
    
    std::getline(ss, term_str, '|');
    std::getline(ss, index_str, '|');
    std::getline(ss, mutation_str);
    
    uint64_t term = std::stoull(term_str);
    uint64_t index = std::stoull(index_str);
    Mutation mutation = Mutation::deserialize(mutation_str);
    
    return LogEntry(term, index, mutation);
}

} // namespace raft
