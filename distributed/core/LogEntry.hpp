#pragma once

#include "Mutation.hpp"
#include <string>
#include <cstdint>

namespace raft {

/**
 * Class representing an entry in the Raft log
 */
class LogEntry {
public:
    /**
     * Default constructor required for STL containers
     */
    LogEntry() : term_(0), index_(0), mutation_(MutationType::NOOP, "") {}
    
    /**
     * Constructor
     * @param term The term in which the entry was created
     * @param index The index of the entry in the log
     * @param mutation The mutation to apply
     */
    LogEntry(uint64_t term, uint64_t index, const Mutation& mutation)
        : term_(term), index_(index), mutation_(mutation) {}
    
    /**
     * Get the term of this entry
     * @return The term
     */
    uint64_t term() const { return term_; }
    
    /**
     * Get the index of this entry
     * @return The index
     */
    uint64_t index() const { return index_; }
    
    /**
     * Get the mutation contained in this entry
     * @return The mutation
     */
    const Mutation& mutation() const { return mutation_; }
    
    /**
     * Serialize the log entry to a string for storage/transmission
     * @return Serialized log entry
     */
    std::string serialize() const;
    
    /**
     * Deserialize a log entry from a string
     * @param serialized The serialized log entry
     * @return Deserialized log entry
     */
    static LogEntry deserialize(const std::string& serialized);

private:
    uint64_t term_;
    uint64_t index_;
    Mutation mutation_;
};

} // namespace raft
