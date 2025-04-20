#include "csv_service_impl.hpp"
#include "storage/csv_parser.hpp"
#include "core/TableView.hpp"
#include "core/Mutation.hpp"

using grpc::ServerContext;
using grpc::Status;

Status CsvServiceImpl::UploadCsv(
    ServerContext* context,
    const csvservice::CsvUploadRequest* request,
    csvservice::CsvUploadResponse* response) {
    
    std::string filename = request->filename();
    std::string csv_data(request->csv_data());
    
    ColumnStore column_store = csv::parse_csv(csv_data);
    
    // For now, we're directly using the InMemoryStateMachine interface
    // In the future, this could be switched to a distributed implementation
    auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        mem_state->add_csv_file(filename, column_store.column_names, column_store.columns);
    }
    
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
    
    // Get the list of files from the state machine
    auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        for (const auto& filename : mem_state->list_files()) {
            response->add_filenames(filename);
        }
    }
    
    return Status::OK;
}

Status CsvServiceImpl::ViewFile(
    ServerContext* context,
    const csvservice::ViewFileRequest* request,
    csvservice::ViewFileResponse* response) {
    
    const std::string& filename = request->filename();
    
    // Get a view of the file from the state machine
    TableView view = state_->view(filename);
    
    if (view.empty()) {
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    response->set_success(true);
    response->set_message("File content retrieved successfully");
    
    // Add column names
    for (const auto& column_name : view.column_names) {
        response->add_column_names(column_name);
    }
    
    // If there are no columns, we're done
    if (view.column_names.empty()) {
        return Status::OK;
    }
    
    // Get the number of rows (assuming all columns have the same size)
    const auto& first_column = view.columns.at(view.column_names[0]);
    size_t row_count = first_column.size();
    
    // For each row, create a Row message with values from each column
    for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
        auto* row = response->add_rows();
        
        for (const auto& column_name : view.column_names) {
            const auto& column = view.columns.at(column_name);
            if (row_idx < column.size()) {
                row->add_values(column[row_idx]);
            } else {
                row->add_values("");  // Handle if columns have different sizes
            }
        }
    }
    
    return Status::OK;
}

Status CsvServiceImpl::ComputeSum(
    ServerContext* context,
    const csvservice::ColumnOperationRequest* request,
    csvservice::NumericResponse* response) {
    
    const std::string& filename = request->filename();
    const std::string& column_name = request->column_name();
    
    // Get a view of the file from the state machine
    TableView view = state_->view(filename);
    
    if (view.empty()) {
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    try {
        double sum = view.compute_sum(column_name);
        response->set_success(true);
        response->set_message("Sum computed successfully");
        response->set_value(sum);
    } catch (const std::exception& e) {
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
    
    // Get a view of the file from the state machine
    TableView view = state_->view(filename);
    
    if (view.empty()) {
        response->set_success(false);
        response->set_message("File not found");
        return Status::OK;
    }
    
    try {
        double avg = view.compute_average(column_name);
        response->set_success(true);
        response->set_message("Average computed successfully");
        response->set_value(avg);
    } catch (const std::exception& e) {
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
    
    // Create a mutation for inserting a row
    Mutation mutation;
    mutation.file = filename;
    
    RowInsert insert;
    for (int i = 0; i < request->values_size(); ++i) {
        insert.values.push_back(request->values(i));
    }
    mutation.op = insert;
    
    try {
        // Apply the mutation to the state machine
        state_->apply(mutation);
        response->set_success(true);
        response->set_message("Row inserted successfully");
    } catch (const std::exception& e) {
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
    
    // Create a mutation for deleting a row
    Mutation mutation;
    mutation.file = filename;
    
    RowDelete del;
    del.row_index = row_index;
    mutation.op = del;
    
    try {
        // Apply the mutation to the state machine
        state_->apply(mutation);
        response->set_success(true);
        response->set_message("Row deleted successfully");
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_message(e.what());
    }
    
    return Status::OK;
}

void CsvServiceImpl::update_loaded_files_cache() const {
    // Clear existing cache before updating
    const_cast<CsvServiceImpl*>(this)->loaded_files.clear();
    
    // Get a list of files from the state machine
    auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
    if (mem_state) {
        for (const auto& filename : mem_state->list_files()) {
            // Get a view of the file from the state machine
            TableView view = state_->view(filename);
            
            // Create a ColumnStore from the TableView
            ColumnStore store;
            store.column_names = view.column_names;
            store.columns = view.columns;
            
            // Update the loaded_files map
            const_cast<CsvServiceImpl*>(this)->loaded_files[filename] = store;
        }
    }
}

const std::unordered_map<std::string, ColumnStore>& CsvServiceImpl::get_loaded_files() const {
    // Update the cache before returning
    update_loaded_files_cache();
    return loaded_files;
}
