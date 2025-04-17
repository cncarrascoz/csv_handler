// gRPC service implementation for CSV handling
#pragma once

#include "storage/column_store.hpp"
#include "proto/csv_service.grpc.pb.h"

#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>

class CsvServiceImpl final : public csvservice::CsvService::Service {
public:
    // Use a shared_mutex for reader-writer lock pattern
    // Multiple clients can read simultaneously, but writes are exclusive
    mutable std::shared_mutex files_mutex;
    std::unordered_map<std::string, ColumnStore> loaded_files;

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
};
