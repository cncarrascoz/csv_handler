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
    
    // Exclusive lock for writing
    std::unique_lock<std::shared_mutex> lock(files_mutex);
    loaded_files[filename] = column_store;
    lock.unlock();
    
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
    
    // Shared lock for reading
    std::shared_lock<std::shared_mutex> lock(files_mutex);
    for (const auto& entry : loaded_files) {
        response->add_filenames(entry.first);
    }
    
    return Status::OK;
}

Status CsvServiceImpl::ViewFile(
    ServerContext* context,
    const csvservice::ViewFileRequest* request,
    csvservice::ViewFileResponse* response) {
    
    const std::string& filename = request->filename();
    
    // Shared lock for reading
    std::shared_lock<std::shared_mutex> lock(files_mutex);
    auto it = loaded_files.find(filename);
    
    if (it == loaded_files.end()) {
        lock.unlock();
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    const ColumnStore& store = it->second;
    response->set_success(true);
    response->set_message("File content retrieved successfully");
    
    // Add column names
    for (const auto& column_name : store.column_names) {
        response->add_column_names(column_name);
    }
    
    // If there are no columns, we're done
    if (store.column_names.empty()) {
        return Status::OK;
    }
    
    // Get the number of rows (assuming all columns have the same size)
    const auto& first_column = store.columns.at(store.column_names[0]);
    size_t row_count = first_column.size();
    
    // For each row, create a Row message with values from each column
    for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
        auto* row = response->add_rows();
        
        for (const auto& column_name : store.column_names) {
            const auto& column = store.columns.at(column_name);
            if (row_idx < column.size()) {
                row->add_values(column[row_idx]);
            } else {
                row->add_values("");  // Handle if columns have different sizes
            }
        }
    }
    lock.unlock();
    
    return Status::OK;
}

Status CsvServiceImpl::ComputeSum(
    ServerContext* context,
    const csvservice::ColumnOperationRequest* request,
    csvservice::NumericResponse* response) {
    
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    // Shared lock for reading
    std::shared_lock<std::shared_mutex> lock(files_mutex);
    auto it = loaded_files.find(filename);
    if (it == loaded_files.end()) {
        lock.unlock();
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    const ColumnStore& store = it->second;
    try {
        double sum = compute_sum(store, column_name);
        lock.unlock();
        response->set_success(true);
        response->set_message("Sum computed successfully");
        response->set_value(sum);
    } catch (const std::exception& e) {
        lock.unlock();
        response->set_success(false);
        response->set_message(e.what());
    }
    
    return Status::OK;
}

Status CsvServiceImpl::ComputeAverage(
    ServerContext* context,
    const csvservice::ColumnOperationRequest* request,
    csvservice::NumericResponse* response) {
    
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    // Shared lock for reading
    std::shared_lock<std::shared_mutex> lock(files_mutex);
    auto it = loaded_files.find(filename);
    if (it == loaded_files.end()) {
        lock.unlock();
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    const ColumnStore& store = it->second;
    try {
        double avg = compute_average(store, column_name);
        lock.unlock();
        response->set_success(true);
        response->set_message("Average computed successfully");
        response->set_value(avg);
    } catch (const std::exception& e) {
        lock.unlock();
        response->set_success(false);
        response->set_message(e.what());
    }
    
    return Status::OK;
}

Status CsvServiceImpl::InsertRow(
    ServerContext* context,
    const csvservice::InsertRowRequest* request,
    csvservice::ModificationResponse* response) {
    
    const std::string& filename = request->filename();
    
    // Exclusive lock for writing
    std::unique_lock<std::shared_mutex> lock(files_mutex);
    auto it = loaded_files.find(filename);
    
    if (it == loaded_files.end()) {
        lock.unlock();
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    ColumnStore& store = it->second;
    std::vector<std::string> values;
    
    for (int i = 0; i < request->values_size(); ++i) {
        values.push_back(request->values(i));
    }
    
    try {
        insert_row(store, values);
        lock.unlock();
        response->set_success(true);
        response->set_message("Row inserted successfully");
    } catch (const std::exception& e) {
        lock.unlock();
        response->set_success(false);
        response->set_message(e.what());
    }
    
    return Status::OK;
}

Status CsvServiceImpl::DeleteRow(
    ServerContext* context,
    const csvservice::DeleteRowRequest* request,
    csvservice::ModificationResponse* response) {
    
    const std::string& filename = request->filename();
    int row_index = request->row_index();
    
    // Exclusive lock for writing
    std::unique_lock<std::shared_mutex> lock(files_mutex);
    auto it = loaded_files.find(filename);
    
    if (it == loaded_files.end()) {
        lock.unlock();
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    ColumnStore& store = it->second;
    
    try {
        delete_row(store, row_index);
        lock.unlock();
        response->set_success(true);
        response->set_message("Row deleted successfully");
    } catch (const std::exception& e) {
        lock.unlock();
        response->set_success(false);
        response->set_message(e.what());
    }
    
    return Status::OK;
}
