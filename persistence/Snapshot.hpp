#pragma once

#include "core/TableView.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

/**
 * Snapshot represents a point-in-time copy of the entire state machine
 * Used for durability and for bootstrapping new/restarted nodes
 */
class Snapshot {
public:
    struct FileData {
        std::vector<std::string> column_names;
        std::unordered_map<std::string, std::vector<std::string>> columns;
    };

    /**
     * Constructor
     */
    Snapshot() = default;

    /**
     * Create a snapshot from state data
     * @param files Map of filenames to their data
     * @param last_index The WAL index this snapshot represents
     */
    Snapshot(std::unordered_map<std::string, FileData> files, uint64_t last_index);

    /**
     * Save snapshot to disk
     * @param directory Directory to save the snapshot in
     * @return True if successful, false otherwise
     */
    bool save(const std::string& directory) const;

    /**
     * Load snapshot from disk
     * @param directory Directory containing the snapshot
     * @return Loaded snapshot or empty snapshot if not found
     */
    static Snapshot load(const std::string& directory);

    /**
     * Get the WAL index this snapshot represents
     * @return Last included WAL index
     */
    uint64_t last_included_index() const;

    /**
     * Get list of files in the snapshot
     * @return Vector of filenames
     */
    std::vector<std::string> file_list() const;

    /**
     * Get data for a specific file in the snapshot
     * @param filename Name of the file to retrieve
     * @return FileData for the requested file or empty if not found
     */
    FileData get_file_data(const std::string& filename) const;

private:
    std::unordered_map<std::string, FileData> files_;
    uint64_t last_included_index_ = 0;
};
