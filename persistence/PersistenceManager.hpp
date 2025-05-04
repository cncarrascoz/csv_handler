#pragma once

#include "core/IStateMachine.hpp"
#include "storage/InMemoryStateMachine.hpp"
#include "DurableStateMachine.hpp"
#include <memory>
#include <string>
#include <mutex>
#include <atomic>

/**
 * PersistenceManager acts as a "sidecar" process that observes mutations
 * applied to an InMemoryStateMachine and persists them to disk using a DurableStateMachine.
 * It also handles recovery on server startup.
 */
class PersistenceManager {
public:
    /**
     * Constructor
     * @param mem_state Pointer to the InMemoryStateMachine to observe
     * @param data_dir Directory where data files will be stored
     * @param log_dir Directory where WAL files will be stored
     */
    PersistenceManager(
        std::shared_ptr<InMemoryStateMachine> mem_state,
        const std::string& data_dir = "data",
        const std::string& log_dir = "logs");
    
    /**
     * Destructor
     */
    ~PersistenceManager();
    
    /**
     * Initialize the persistence manager
     * This will recover state from disk if available
     * @return True if initialization was successful
     */
    bool initialize();
    
    /**
     * Observe a mutation applied to the InMemoryStateMachine
     * This will persist the mutation to disk
     * @param mutation The mutation to observe
     */
    void observe_mutation(const Mutation& mutation);
    
    /**
     * Create a snapshot of the current state
     * @return True if the snapshot was created successfully
     */
    bool create_snapshot();
    
    /**
     * Get the last applied index
     * @return The index of the last applied mutation
     */
    uint64_t last_applied_index() const;

private:
    std::shared_ptr<InMemoryStateMachine> mem_state_;
    std::unique_ptr<DurableStateMachine> durable_state_;
    std::string data_dir_;
    std::string log_dir_;
    std::atomic<uint64_t> last_applied_index_ = 0;
    mutable std::mutex mutex_;
};
