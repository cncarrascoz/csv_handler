#include "csv_service_impl.hpp"
#include "storage/csv_parser.hpp"
#include "core/TableView.hpp"
#include "core/Mutation.hpp"
#include "distributed/core/Mutation.hpp"

using grpc::ServerContext;
using grpc::Status;

Status CsvServiceImpl::UploadCsv(
    ServerContext* context,
    const csvservice::CsvUploadRequest* request,
    csvservice::CsvUploadResponse* response) {
    
    std::string filename = request->filename();
    std::string csv_data(request->csv_data());
    
    if (distributed_mode_ && raft_node_) {
        // In distributed mode, create a mutation and submit it to the Raft node
        raft::Mutation mutation(raft::MutationType::UPLOAD_CSV, filename, csv_data);
        bool success = raft_node_->submit(mutation);
        
        if (success) {
            // The mutation was successfully submitted to the Raft log
            response->set_success(true);
            response->set_message("CSV file uploaded successfully via Raft consensus");
            
            // Parse the CSV to get row and column counts for the response
            ColumnStore column_store = csv::parse_csv(csv_data);
            response->set_row_count(column_store.columns.empty() ? 0 : 
                                  column_store.columns.begin()->second.size());
            response->set_column_count(column_store.column_names.size());
        } else {
            response->set_success(false);
            response->set_message("Failed to submit CSV upload to Raft log");
        }
    } else {
        // Fallback to non-distributed mode
        ColumnStore column_store = csv::parse_csv(csv_data);
        
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
        if (mem_state) {
            mem_state->add_csv_file(filename, column_store.column_names, column_store.columns);
        }
        
        response->set_success(true);
        response->set_message("CSV file loaded successfully in column-store format (non-distributed mode)");
        response->set_row_count(column_store.columns.empty() ? 0 : 
                              column_store.columns.begin()->second.size());
        response->set_column_count(column_store.column_names.size());
    }
    
    return Status::OK;
}

Status CsvServiceImpl::ListLoadedFiles(
    ServerContext* context,
    const csvservice::Empty* request,
    csvservice::CsvFileList* response) {
    
    if (distributed_mode_ && raft_node_) {
        // In distributed mode, get the list of files from the Raft state machine
        auto csv_state_machine = std::dynamic_pointer_cast<raft::CsvStateMachine>(raft_node_->state_machine());
        if (csv_state_machine) {
            for (const auto& filename : csv_state_machine->list_files()) {
                response->add_filenames(filename);
            }
        }
    } else {
        // Fallback to non-distributed mode
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
        if (mem_state) {
            for (const auto& filename : mem_state->list_files()) {
                response->add_filenames(filename);
            }
        }
    }
    
    return Status::OK;
}

Status CsvServiceImpl::ViewFile(
    ServerContext* context, 
    const csvservice::ViewFileRequest* request,
    csvservice::ViewFileResponse* response) {
    
    std::string filename = request->filename();
    
    if (distributed_mode_ && raft_node_) {
        // In distributed mode, get the file from the Raft state machine
        auto csv_state_machine = std::dynamic_pointer_cast<raft::CsvStateMachine>(raft_node_->state_machine());
        if (csv_state_machine) {
            auto file_data = csv_state_machine->get_file(filename);
            if (!file_data.first.empty()) {
                response->set_success(true);
                
                // Add column names
                for (const auto& col_name : file_data.first) {
                    response->add_column_names(col_name);
                }
                
                // Add rows
                size_t num_rows = file_data.second.empty() ? 0 : 
                                file_data.second.begin()->second.size();
                
                for (size_t i = 0; i < num_rows; ++i) {
                    auto* row = response->add_rows();
                    for (const auto& col_name : file_data.first) {
                        const auto& col_data = file_data.second.at(col_name);
                        row->add_values(col_data[i]);
                    }
                }
            } else {
                response->set_success(false);
                response->set_message("File not found: " + filename);
            }
        } else {
            response->set_success(false);
            response->set_message("Failed to access Raft state machine");
        }
    } else {
        // Fallback to non-distributed mode
        auto* mem_state = dynamic_cast<InMemoryStateMachine*>(state_.get());
        if (mem_state) {
            // Use the view method instead of get_file
            auto table_view = mem_state->view(filename);
            if (!table_view.empty()) {
                response->set_success(true);
                
                // Add column names
                for (const auto& col_name : table_view.column_names) {
                    response->add_column_names(col_name);
                }
                
                // Add rows
                size_t num_rows = table_view.row_count();
                for (size_t row = 0; row < num_rows; ++row) {
                    auto* csv_row = response->add_rows();
                    for (const auto& col_name : table_view.column_names) {
                        if (table_view.columns.count(col_name) > 0 && 
                            row < table_view.columns.at(col_name).size()) {
                            csv_row->add_values(table_view.columns.at(col_name)[row]);
                        } else {
                            csv_row->add_values("");  // Empty value for missing data
                        }
                    }
                }
            } else {
                response->set_success(false);
                response->set_message("File not found: " + filename);
            }
        } else {
            response->set_success(false);
            response->set_message("Invalid state machine");
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
