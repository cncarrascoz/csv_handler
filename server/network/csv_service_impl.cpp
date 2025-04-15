#include "csv_service_impl.hpp"
#include "storage/csv_parser.hpp"

using grpc::ServerContext;
using grpc::Status;

Status CsvServiceImpl::UploadCsv(
    ServerContext* context,
    const csvservice::CsvUploadRequest* request,
    csvservice::CsvUploadResponse* response) {
    
    std::string filename = request->filename();
    std::string csv_data(request->csv_data());
    
    ColumnStore column_store = csv::parse_csv(csv_data);
    loaded_files[filename] = column_store;
    
    response->set_success(true);
    response->set_message("CSV file loaded successfully in column-store format");
    response->set_row_count(column_store.columns.empty() ? 0 : 
                          column_store.columns.begin()->second.size());
    response->set_column_count(column_store.column_names.size());
    
    return Status::OK;
}

Status CsvServiceImpl::ListLoadedFiles(
    ServerContext* context,
    const csvservice::Empty* request,
    csvservice::CsvFileList* response) {
    
    for (const auto& entry : loaded_files) {
        response->add_filenames(entry.first);
    }
    
    return Status::OK;
}
