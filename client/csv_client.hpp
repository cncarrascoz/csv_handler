// client/csv_client.hpp: Defines the CsvClient class for interacting with the server.
#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "proto/csv_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using csvservice::CsvService;
using csvservice::CsvUploadRequest;
using csvservice::CsvUploadResponse;
using csvservice::Empty;
using csvservice::CsvFileList;

class CsvClient {
public:
    CsvClient(std::shared_ptr<Channel> channel);

    // Attempts to upload a CSV file to the server.
    // Returns true on success, false otherwise.
    bool UploadCsv(const std::string& filename);

    // Requests a list of loaded filenames from the server.
    // Prints the list to stdout.
    void ListFiles();

private:
    std::unique_ptr<CsvService::Stub> stub_;
};
