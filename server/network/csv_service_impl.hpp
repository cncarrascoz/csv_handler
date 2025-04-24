// gRPC service implementation for CSV handling
#pragma once

#include "core/IStateMachine.hpp"
#include "storage/InMemoryStateMachine.hpp"
#include "storage/column_store.hpp" // Include for ColumnStore backward compatibility
#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "proto/csv_service.grpc.pb.h" // gRPC service definition
#include "proto/csv_service.pb.h"      // Protobuf message definitions
#include "server_registry.hpp"

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

    // RPC called by leader to replicate upload to peers
    grpc::Status ReplicateUpload(grpc::ServerContext* context, const csvservice::CsvUploadRequest* request,
                                 csvservice::ReplicateUploadResponse* response) override;

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
    std::unique_ptr<IStateMachine> state_;
    
    // Reference to the ServerRegistry
    ServerRegistry& registry_;
    
    // Update loaded_files_cache for backward compatibility
    void update_loaded_files_cache() const;
    
    // Forward a request to the leader if this server is not the leader
    template<typename RequestType, typename ResponseType>
    bool forward_to_leader_if_needed(
        const RequestType* request,
        ResponseType* response,
        std::function<grpc::Status(const RequestType*, ResponseType*)> handler);
};

} // namespace network
