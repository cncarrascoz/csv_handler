// gRPC service implementation for CSV handling
#pragma once

#include "core/IStateMachine.hpp"
#include "storage/InMemoryStateMachine.hpp"
#include "storage/column_store.hpp" // Include for ColumnStore backward compatibility
#include "proto/csv_service.grpc.pb.h"

#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>

class CsvServiceImpl final : public csvservice::CsvService::Service {
public:
    CsvServiceImpl() : state_(std::make_unique<InMemoryStateMachine>()) {}
    
    // Customizable constructor for dependency injection (useful for testing or using DurableStateMachine)
    explicit CsvServiceImpl(std::unique_ptr<IStateMachine> state) : state_(std::move(state)) {}

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
    
    // Update loaded_files from state_machine for backward compatibility
    void update_loaded_files_cache() const;
};
