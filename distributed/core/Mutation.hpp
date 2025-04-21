#pragma once

#include <string>
#include <cstdint>

namespace raft {

/**
 * Enum defining the types of mutations that can be applied to the state machine
 */
enum class MutationType {
    NOOP,       // No operation, used for leader election
    UPLOAD_CSV, // Upload a new CSV file
    INSERT_ROW, // Insert a row into a CSV file
    DELETE_ROW, // Delete a row from a CSV file
    UPDATE_CELL // Update a cell in a CSV file
};

/**
 * Class representing a mutation to be applied to the state machine
 */
class Mutation {
public:
    /**
     * Default constructor
     */
    Mutation() : type_(MutationType::NOOP), filename_(""), data_("") {}
    
    /**
     * Constructor
     * @param type The type of mutation
     * @param filename The filename to operate on
     * @param data The data for the mutation
     */
    Mutation(MutationType type, const std::string& filename, const std::string& data = "")
        : type_(type), filename_(filename), data_(data) {}
    
    /**
     * Get the type of this mutation
     * @return The mutation type
     */
    MutationType type() const { return type_; }
    
    /**
     * Get the filename this mutation operates on
     * @return The filename
     */
    const std::string& filename() const { return filename_; }
    
    /**
     * Get the data for this mutation
     * @return The mutation data
     */
    const std::string& data() const { return data_; }
    
    /**
     * Serialize the mutation to a string for storage/transmission
     * @return Serialized mutation
     */
    std::string serialize() const;
    
    /**
     * Deserialize a mutation from a string
     * @param serialized The serialized mutation
     * @return Deserialized mutation
     */
    static Mutation deserialize(const std::string& serialized);

private:
    MutationType type_;
    std::string filename_;
    std::string data_;
};

} // namespace raft
