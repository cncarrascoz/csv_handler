#pragma once

#include "Mutation.hpp"
#include <string>

namespace raft {

/**
 * Interface for state machines in the Raft consensus algorithm
 */
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    
    /**
     * Apply a mutation to the state machine
     * @param mutation The mutation to apply
     * @return True if the mutation was applied successfully
     */
    virtual bool apply(const Mutation& mutation) = 0;
    
    /**
     * Create a snapshot of the current state
     * @return A serialized representation of the current state
     */
    virtual std::string create_snapshot() = 0;
    
    /**
     * Restore the state from a snapshot
     * @param snapshot The serialized state to restore from
     * @return True if the state was restored successfully
     */
    virtual bool restore_snapshot(const std::string& snapshot) = 0;
};

} // namespace raft
