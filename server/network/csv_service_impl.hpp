// gRPC service implementation for CSV handling
#pragma once

// Standard library includes
#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

// gRPC / Protobuf includes
#include <grpcpp/grpcpp.h> // Include base gRPC headers
#include "proto/csv_service.grpc.pb.h" // Provides gRPC service & message definitions (including .pb.h)
#include "proto/csv_service.pb.h"      // Protobuf message definitions

// Project includes
#include "core/IStateMachine.hpp" // Provides IStateMachine interface and Mutation struct
#include "server/network/server_registry.hpp" // Provides network::ServerRegistry
#include "storage/column_store.hpp"         // Provides storage::ColumnStore
#include "storage/InMemoryStateMachine.hpp" // Provides InMemoryStateMachine
#include "persistence/PersistenceManager.hpp" // Provides PersistenceManager

// Use specific gRPC types for clarity
using grpc::Status;

namespace network {

class CsvServiceImpl final : public csvservice::CsvService::Service {
public:
    CsvServiceImpl(ServerRegistry& registry); // Constructor accepting registry
    
    // Destructor
    ~CsvServiceImpl();
    
    // Initialize the service
    void initialize();
    
    // Get the loaded files (for menu commands)
    const std::unordered_map<std::string, ColumnStore>& get_loaded_files() const {
        return loaded_files_cache_;
    }
    
    // Mutex for thread-safe access to files
    mutable std::shared_mutex files_mutex_;
    
    // Deleted default constructor to enforce registry injection
    CsvServiceImpl() = delete;
    
    // Move operations are implicitly deleted because of std::shared_mutex member
    // CsvServiceImpl(CsvServiceImpl&&) = default; 
    // CsvServiceImpl& operator=(CsvServiceImpl&&) = default; 
 
    // Disallow copying
    CsvServiceImpl(const CsvServiceImpl&) = delete;
    CsvServiceImpl& operator=(const CsvServiceImpl&) = delete;

    // Initialize with server registry (removed, logic moved to constructor)
    // void initialize(const std::string& server_address, const std::vector<std::string>& peer_addresses = {});
    
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
        
    // New method to check server health and get cluster status
    grpc::Status GetClusterStatus(
        grpc::ServerContext* context,
        const csvservice::Empty* request,
        csvservice::ClusterStatusResponse* response) override;

    grpc::Status RegisterPeer(grpc::ServerContext* context, const csvservice::RegisterPeerRequest* request, csvservice::RegisterPeerResponse* response) override;
    grpc::Status Heartbeat(grpc::ServerContext* context, const csvservice::HeartbeatRequest* request, csvservice::HeartbeatResponse* response) override;

    // RPC called by leader to replicate upload to peers
    grpc::Status ReplicateUpload(grpc::ServerContext* context, const csvservice::CsvUploadRequest* request,
                                 csvservice::ReplicateUploadResponse* response) override;

    // Internal RPC used by the leader to replicate a mutation to peers
    grpc::Status ApplyMutation(grpc::ServerContext* context, const csvservice::ReplicateMutationRequest* request,
                                 csvservice::ReplicateMutationResponse* response) override;

    // Handle leader change notification from ServerRegistry
    void handle_leader_change(const std::string& new_leader);

    // Helper to get loaded files for menu.cpp
    // Removed duplicate method

private:
    // Reference to the server registry for leader election and peer management
    ServerRegistry& registry_;

    // Shared mutex for thread-safe access to state
    mutable std::shared_mutex mutex_;

    // The state machine that manages the CSV data
    std::shared_ptr<InMemoryStateMachine> state_;

    // The persistence manager that handles durability
    std::unique_ptr<PersistenceManager> persistence_manager_;

    // Cache of loaded files for quick access
    mutable std::unordered_map<std::string, ColumnStore> loaded_files_cache_;

    // Helper method to update the loaded files cache
    void update_loaded_files_cache();

    // Forward a request to the leader if this server is not the leader
    template<typename RequestType, typename ResponseType, typename HandlerType>
    bool forward_to_leader_if_needed(
        const RequestType* request,
        ResponseType* response,
        HandlerType handler)
    {
        // If we are the leader, no need to forward
        if (registry_.is_leader()) {
            return false;
        }

        std::string leader_address = registry_.get_leader();
        if (leader_address.empty()) {
            response->set_success(false);
            response->set_message("No leader available. Try again later.");
            return true;
        }

        std::cout << "Forwarding request to leader at " << leader_address << std::endl;

        // Create a channel to the leader
        auto channel = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
        auto stub = csvservice::CsvService::NewStub(channel);

        // Set up context with timeout
        grpc::ClientContext context;
        std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() + std::chrono::seconds(5);
        context.set_deadline(deadline);

        // Forward the request to the leader
        Status status = handler(request, response);

        if (!status.ok()) {
            response->set_success(false);
            response->set_message("Failed to forward request to leader: " +
                                 status.error_message());
            return true;
        }

        return true;
    }

    // Helper to replicate mutations (insert/delete) asynchronously
    void replicate_mutation_async(const Mutation& mutation);
};

} // namespace network
