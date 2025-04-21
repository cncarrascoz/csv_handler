// gRPC service implementation for CSV handling
#pragma once

#include "core/IStateMachine.hpp"
#include "storage/InMemoryStateMachine.hpp"
#include "storage/column_store.hpp" // Include for ColumnStore backward compatibility
#include "distributed/core/CsvStateMachine.hpp"
#include "distributed/raft_node.hpp"
#include "proto/csv_service.grpc.pb.h"

#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

class CsvServiceImpl final : public csvservice::CsvService::Service {
public:
    // Default constructor - uses InMemoryStateMachine (non-distributed)
    CsvServiceImpl() : state_(std::make_unique<InMemoryStateMachine>()),
                      raft_node_(nullptr),
                      distributed_mode_(false) {}
    
    // Constructor for distributed mode with Raft consensus
    CsvServiceImpl(const std::string& node_id, 
                  const std::vector<std::string>& peer_addresses,
                  const std::string& storage_path) 
        : distributed_mode_(true) {
        
        // Create a CsvStateMachine for Raft
        auto csv_state_machine = std::make_shared<raft::CsvStateMachine>();
        
        // Create the RaftNode with the CsvStateMachine
        raft_node_ = std::make_unique<RaftNode>(
            node_id,
            csv_state_machine,
            peer_addresses,
            storage_path
        );
        
        // Start the Raft node
        raft_node_->start();
        
        // Also create a local state machine for backward compatibility
        state_ = std::make_unique<InMemoryStateMachine>();
        
        std::cout << "Server started in distributed mode with Raft consensus" << std::endl;
        std::cout << "Node ID: " << node_id << std::endl;
        std::cout << "Peer addresses: ";
        for (const auto& addr : peer_addresses) {
            std::cout << addr << " ";
        }
        std::cout << std::endl;
    }
    
    // Customizable constructor for dependency injection (useful for testing)
    explicit CsvServiceImpl(std::unique_ptr<IStateMachine> state) 
        : state_(std::move(state)), 
          raft_node_(nullptr),
          distributed_mode_(false) {}

    ~CsvServiceImpl() {
        if (raft_node_) {
            raft_node_->stop();
        }
    }

    grpc::Status UploadCsv(
        grpc::ServerContext* context,
        const csvservice::CsvUploadRequest* request,
        csvservice::CsvUploadResponse* response) override;
        
    grpc::Status ListLoadedFiles(
        grpc::ServerContext* context,
        const csvservice::Empty* request,
        csvservice::CsvFileList* response) override;
        
    // New RPCs to handle extended functionality
    grpc::Status ViewFile(
        grpc::ServerContext* context, 
        const csvservice::ViewFileRequest* request,
        csvservice::ViewFileResponse* response) override;
        
    grpc::Status ComputeSum(
        grpc::ServerContext* context,
        const csvservice::ColumnOperationRequest* request,
        csvservice::NumericResponse* response) override;
        
    grpc::Status ComputeAverage(
        grpc::ServerContext* context,
        const csvservice::ColumnOperationRequest* request,
        csvservice::NumericResponse* response) override;
        
    grpc::Status InsertRow(
        grpc::ServerContext* context,
        const csvservice::InsertRowRequest* request,
        csvservice::ModificationResponse* response) override;
        
    grpc::Status DeleteRow(
        grpc::ServerContext* context,
        const csvservice::DeleteRowRequest* request,
        csvservice::ModificationResponse* response) override;
    
    /**
     * Get the loaded files (thread-safe)
     * @return Map of loaded files
     */
    const std::unordered_map<std::string, ColumnStore>& get_loaded_files() const {
        // Update the cache before returning
        update_loaded_files_cache();
        return loaded_files;
    }

    /**
     * Lock the files mutex for reading
     * @return A shared lock on the files mutex
     */
    std::shared_lock<std::shared_mutex> lock_files_for_reading() const {
        return std::shared_lock<std::shared_mutex>(files_mutex_);
    }
    
    // Get the state machine (for testing/debugging)
    std::shared_ptr<raft::CsvStateMachine> get_raft_state_machine() const {
        if (raft_node_) {
            return std::dynamic_pointer_cast<raft::CsvStateMachine>(raft_node_->state_machine());
        }
        return nullptr;
    }
    
private:
    // Update the loaded_files cache from the state machine
    void update_loaded_files_cache() const;
    
    std::unique_ptr<IStateMachine> state_;
    std::unique_ptr<RaftNode> raft_node_;
    bool distributed_mode_;
    
    // Cache of loaded files for backward compatibility
    mutable std::shared_mutex files_mutex_;
    mutable std::unordered_map<std::string, ColumnStore> loaded_files;
};
