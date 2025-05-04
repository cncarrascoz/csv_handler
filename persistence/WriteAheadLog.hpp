#pragma once

#include "core/Mutation.hpp"
#include <string>
#include <vector>
#include <mutex>

/**
 * Write-ahead log for durably storing mutations before applying them
 * This implementation persists mutations to disk for durability
 */
class WriteAheadLog {
public:
    /**
     * Constructor
     * @param log_dir Directory where log files will be stored
     */
    explicit WriteAheadLog(const std::string& log_dir = "logs");
    
    /**
     * Append a mutation to the log
     * @param mutation The mutation to append
     * @return The log index of the appended mutation
     */
    uint64_t append(const Mutation& mutation);
    
    /**
     * Get all mutations since a specific index
     * @param since_index Start index (exclusive)
     * @return Vector of mutations
     */
    std::vector<Mutation> get_since(uint64_t since_index) const;
    
    /**
     * Get the current last index in the log
     * @return The index of the last mutation
     */
    uint64_t last_index() const;
    
    /**
     * Truncate the log up to (and including) the given index
     * This is typically done after a snapshot is created
     * @param up_to_index The index up to which to truncate
     */
    void truncate(uint64_t up_to_index);

private:
    std::string log_dir_;
    mutable std::mutex mutex_;
    uint64_t last_index_ = 0;
    
    // In-memory cache of mutations
    std::vector<Mutation> mutations_;
    
    // Helper methods for serialization
    std::string serialize_mutation(const Mutation& mutation) const;
    Mutation deserialize_mutation(const std::string& serialized) const;
};
