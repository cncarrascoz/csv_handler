#pragma once

#include <string>
#include <vector>
#include <variant>

/**
 * Row insertion operation
 */
struct RowInsert {
    std::vector<std::string> values;
};

/**
 * Row deletion operation
 */
struct RowDelete {
    int row_index;
};

/**
 * Mutation represents a state-changing operation to be applied to the state machine
 */
struct Mutation {
    std::string file;
    std::variant<RowInsert, RowDelete> op;
    
    // Helper methods to check operation type
    bool has_insert() const { return std::holds_alternative<RowInsert>(op); }
    bool has_delete() const { return std::holds_alternative<RowDelete>(op); }
    
    // Helper methods to access operation data
    const RowInsert& insert() const { return std::get<RowInsert>(op); }
    const RowDelete& del() const { return std::get<RowDelete>(op); }
};
