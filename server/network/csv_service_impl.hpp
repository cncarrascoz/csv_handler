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

// Use specific gRPC types for clarity
using grpc::Status;

namespace network {

class CsvServiceImpl final : public csvservice::CsvService::Service {
public:
    CsvServiceImpl(ServerRegistry& registry); // Constructor accepting registry

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

    // The following public members are maintained for backward compatibility with CLI
    // They should not be used in new code
    mutable std::shared_mutex files_mutex;
    
    // Gets the current state as a map of ColumnStore objects
    // This is for backward compatibility with menu.cpp
    const std::unordered_map<std::string, ColumnStore>& get_loaded_files() const;
    
    // Backward compatibility alias for get_loaded_files()
    std::unordered_map<std::string, ColumnStore> loaded_files;

private:
    // State machine that implements the core functionality
    std::shared_ptr<IStateMachine> state_; // Use base interface, remove persistence::
    
    // Reference to the ServerRegistry
    ServerRegistry& registry_;
    
    // Update loaded_files_cache for backward compatibility
    void update_loaded_files_cache() const;
    
    // Forward a request to the leader if this server is not the leader
    template<typename RequestType, typename ResponseType>
    bool forward_to_leader_if_needed(
        const RequestType* request,
        ResponseType* response,
        std::function<grpc::Status(const RequestType*, ResponseType*)> handler)
    {
        if (registry_.is_leader()) {
            // This server is the leader, execute locally
            // Note: The handler function passed in will modify the response directly
            return false; // Not forwarded
        }

        // This server is a follower, forward to the leader
        std::string leader_address = registry_.get_leader();
        if (leader_address.empty()) {
            // No leader known, cannot forward
            response->set_success(false);
            response->set_message("Cannot process request: No leader identified.");
            // We might want a more specific gRPC status code here
            // For simplicity, the caller will handle the status based on response->success()
            return true; // Indicate forwarding was attempted but failed pre-flight
        }

        // Create a gRPC channel and stub to communicate with the leader
        auto channel = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
        if (!channel) {
             response->set_success(false);
             response->set_message("Cannot process request: Failed to create channel to leader at " + leader_address);
             return true;
        }
        auto stub = csvservice::CsvService::NewStub(channel);
        if (!stub) {
             response->set_success(false);
             response->set_message("Cannot process request: Failed to create stub for leader at " + leader_address);
             return true;
        }

        grpc::ClientContext context;
        // Set a deadline for the RPC call (e.g., 5 seconds)
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        context.set_deadline(deadline);

        // Determine which RPC method to call on the leader based on request type
        // This requires mapping RequestType to the corresponding stub method.
        // This is a limitation of this generic approach. We need specific logic per request type.
        // For now, assuming InsertRow and DeleteRow are the only forwarded types based on current usage.
        grpc::Status status;
        if constexpr (std::is_same_v<RequestType, csvservice::InsertRowRequest>) {
            // Assume response type is csvservice::ModificationResponse based on InsertRow definition
            if constexpr (std::is_same_v<ResponseType, csvservice::ModificationResponse>) {
                 status = stub->InsertRow(&context, *request, response);
            } else {
                 // Mismatched response type for InsertRow
                 status = grpc::Status(grpc::StatusCode::INTERNAL, "Internal error: Mismatched response type for forwarded InsertRow");
            }
        } else if constexpr (std::is_same_v<RequestType, csvservice::DeleteRowRequest>) {
            // Assume response type is csvservice::ModificationResponse based on DeleteRow definition
            if constexpr (std::is_same_v<ResponseType, csvservice::ModificationResponse>) {
                 status = stub->DeleteRow(&context, *request, response);
            } else {
                 // Mismatched response type for DeleteRow
                  status = grpc::Status(grpc::StatusCode::INTERNAL, "Internal error: Mismatched response type for forwarded DeleteRow");
            }
        } else {
            // Handle other potential request types or indicate an error
            // For now, we only handle InsertRow and DeleteRow forwarding explicitly
            status = grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Forwarding not implemented for this request type");
        }

        // Check the status of the RPC call to the leader
        if (!status.ok()) {
            response->set_success(false);
            response->set_message("Failed to forward request to leader: " + status.error_message());
        } 
        // If status.ok(), the 'response' object should have been populated by the leader's successful execution.
        // The leader's handler would have set success/message.

        return true; // Indicate request was forwarded (or attempt was made)
    }

    // Helper to replicate mutations (insert/delete) asynchronously
    void replicate_mutation_async(const Mutation& mutation); // Remove persistence::

    // Helper to replicate mutations with majority acknowledgment (for fault tolerance)
    // Returns true if a majority of servers acknowledged the mutation, false otherwise
    bool replicate_with_majority_ack(const Mutation& mutation);

    // Helper to replicate file uploads with majority acknowledgment
    // Returns true if a majority of servers acknowledged the upload, false otherwise
    bool replicate_upload_with_majority_ack(const csvservice::CsvUploadRequest& request);
};

} // namespace network
