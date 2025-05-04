#include "Snapshot.hpp"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <sstream>
#include <iomanip>

Snapshot::Snapshot(std::unordered_map<std::string, FileData> files, uint64_t last_index)
    : files_(std::move(files)), last_included_index_(last_index) {}

bool Snapshot::save(const std::string& directory) const {
    // Create the directory if it doesn't exist
    std::filesystem::create_directories(directory);
    
    // Create a snapshot file with metadata
    std::string metadata_path = directory + "/snapshot_metadata.txt";
    std::ofstream metadata_file(metadata_path);
    if (!metadata_file.is_open()) {
        std::cerr << "Failed to open snapshot metadata file for writing: " << metadata_path << std::endl;
        return false;
    }
    
    // Write metadata (last included index and file list)
    metadata_file << "last_included_index=" << last_included_index_ << std::endl;
    metadata_file << "file_count=" << files_.size() << std::endl;
    
    // Save each file's data
    for (const auto& [filename, file_data] : files_) {
        // Write filename to metadata
        metadata_file << "file=" << filename << std::endl;
        
        // Create a file for this file's data
        std::string file_path = directory + "/" + filename + ".snapshot";
        std::ofstream file_out(file_path);
        if (!file_out.is_open()) {
            std::cerr << "Failed to open snapshot file for writing: " << file_path << std::endl;
            return false;
        }
        
        // Write column names
        file_out << "column_count=" << file_data.column_names.size() << std::endl;
        for (const auto& col_name : file_data.column_names) {
            file_out << "column=" << col_name << std::endl;
        }
        
        // Write row count (assuming all columns have the same number of rows)
        size_t row_count = 0;
        if (!file_data.column_names.empty() && !file_data.columns.empty()) {
            const auto& first_col = file_data.columns.at(file_data.column_names[0]);
            row_count = first_col.size();
        }
        file_out << "row_count=" << row_count << std::endl;
        
        // Write column data
        for (const auto& col_name : file_data.column_names) {
            file_out << "data_for=" << col_name << std::endl;
            const auto& col_data = file_data.columns.at(col_name);
            for (const auto& value : col_data) {
                file_out << value << std::endl;
            }
        }
        
        file_out.close();
    }
    
    metadata_file.close();
    std::cout << "Snapshot saved to directory: " << directory << std::endl;
    return true;
}

Snapshot Snapshot::load(const std::string& directory) {
    // Check if directory exists
    if (!std::filesystem::exists(directory)) {
        std::cerr << "Snapshot directory does not exist: " << directory << std::endl;
        return Snapshot();
    }
    
    // Open metadata file
    std::string metadata_path = directory + "/snapshot_metadata.txt";
    if (!std::filesystem::exists(metadata_path)) {
        std::cerr << "Snapshot metadata file does not exist: " << metadata_path << std::endl;
        return Snapshot();
    }
    
    std::ifstream metadata_file(metadata_path);
    if (!metadata_file.is_open()) {
        std::cerr << "Failed to open snapshot metadata file for reading: " << metadata_path << std::endl;
        return Snapshot();
    }
    
    // Read metadata
    uint64_t last_included_index = 0;
    size_t file_count = 0;
    std::vector<std::string> filenames;
    
    std::string line;
    while (std::getline(metadata_file, line)) {
        try {
            if (line.find("last_included_index=") == 0) {
                std::string index_str = line.substr(19);
                if (!index_str.empty()) {
                    last_included_index = std::stoull(index_str);
                }
            } else if (line.find("file_count=") == 0) {
                std::string count_str = line.substr(11);
                if (!count_str.empty()) {
                    file_count = std::stoull(count_str);
                }
            } else if (line.find("file=") == 0) {
                filenames.push_back(line.substr(5));
            }
        } catch (const std::exception& e) {
            std::cerr << "Error parsing metadata line: " << line << " - " << e.what() << std::endl;
            // Continue with the next line
        }
    }
    
    // Load each file's data
    std::unordered_map<std::string, FileData> files;
    for (const auto& filename : filenames) {
        std::string file_path = directory + "/" + filename + ".snapshot";
        if (!std::filesystem::exists(file_path)) {
            std::cerr << "Snapshot file does not exist: " << file_path << std::endl;
            continue;
        }
        
        std::ifstream file_in(file_path);
        if (!file_in.is_open()) {
            std::cerr << "Failed to open snapshot file for reading: " << file_path << std::endl;
            continue;
        }
        
        FileData file_data;
        size_t column_count = 0;
        size_t row_count = 0;
        std::string current_column;
        
        while (std::getline(file_in, line)) {
            try {
                if (line.find("column_count=") == 0) {
                    std::string count_str = line.substr(13);
                    if (!count_str.empty()) {
                        column_count = std::stoull(count_str);
                    }
                } else if (line.find("column=") == 0) {
                    file_data.column_names.push_back(line.substr(7));
                } else if (line.find("row_count=") == 0) {
                    std::string count_str = line.substr(10);
                    if (!count_str.empty()) {
                        row_count = std::stoull(count_str);
                    }
                } else if (line.find("data_for=") == 0) {
                    current_column = line.substr(9);
                    // Initialize the column vector
                    file_data.columns[current_column] = std::vector<std::string>();
                } else if (!current_column.empty()) {
                    // This is a data value for the current column
                    file_data.columns[current_column].push_back(line);
                }
            } catch (const std::exception& e) {
                std::cerr << "Error parsing file data line: " << line << " - " << e.what() << std::endl;
                // Continue with the next line
            }
        }
        
        files[filename] = file_data;
        file_in.close();
    }
    
    std::cout << "Loaded snapshot from directory: " << directory << " with " << files.size() << " files" << std::endl;
    return Snapshot(files, last_included_index);
}

uint64_t Snapshot::last_included_index() const {
    return last_included_index_;
}

std::vector<std::string> Snapshot::file_list() const {
    std::vector<std::string> filenames;
    for (const auto& entry : files_) {
        filenames.push_back(entry.first);
    }
    return filenames;
}

Snapshot::FileData Snapshot::get_file_data(const std::string& filename) const {
    auto it = files_.find(filename);
    if (it != files_.end()) {
        return it->second;
    }
    return FileData(); // Return empty file data if not found
}
