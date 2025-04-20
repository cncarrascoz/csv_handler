#pragma once

#include "Mutation.hpp"
#include <string>
#include <vector>
#include <memory>

/**
 * Interface for a state machine that can apply mutations and provide views
 * This abstraction allows different storage implementations (in-memory, durable, distributed)
 * while maintaining a consistent interface for the server logic.
 */
class IStateMachine {
public:
    /**
     * Apply a mutation to update the state machine
     * @param mutation The mutation to apply
     */
    virtual void apply(const Mutation& mutation) = 0;

    /**
     * Get a view of a specific file in the state machine
     * @param file The name of the file to view
     * @return A view of the file's data
     */
    virtual class TableView view(const std::string& file) const = 0;

    /**
     * Virtual destructor for proper cleanup in derived classes
     */
    virtual ~IStateMachine() = default;
};
