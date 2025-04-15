// gRPC service implementation for CSV handling
#pragma once

#include "storage/column_store.hpp"
#include "proto/csv_service.grpc.pb.h"

#include <unordered_map>
#include <memory>

class CsvServiceImpl final : public csvservice::CsvService::Service {
public:
    std::unordered_map<std::string, ColumnStore> loaded_files;

    grpc::Status UploadCsv(
        grpc::ServerContext* context,
        const csvservice::CsvUploadRequest* request,
        csvservice::CsvUploadResponse* response) override;
        
    grpc::Status ListLoadedFiles(
        grpc::ServerContext* context,
        const csvservice::Empty* request,
        csvservice::CsvFileList* response) override;
};
