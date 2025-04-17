#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "utils/file_utils.hpp"
#include "tests/common/test_utils.hpp"

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdexcept>

using namespace testing;
using namespace csv_test;

class FileUtilsTest : public ::testing::Test {
protected:
    std::string test_dir = "tests/data/";
    std::string test_file = test_dir + "test_file.txt";
    std::string test_content = "Test file content\nLine 2\nLine 3\n";

    void SetUp() override {
        // Create a test file
        std::ofstream file(test_file);
        ASSERT_TRUE(file.is_open()) << "Failed to create test file: " << test_file;
        file << test_content;
        file.close();
    }

    void TearDown() override {
        // Clean up test file
        std::remove(test_file.c_str());
    }
};

TEST_F(FileUtilsTest, ReadFileReturnsCorrectContent) {
    std::string content = file_utils::read_file(test_file);
    EXPECT_EQ(content, test_content);
}

TEST_F(FileUtilsTest, ReadFileThrowsOnNonexistentFile) {
    std::string nonexistent_file = test_dir + "nonexistent.txt";
    EXPECT_THROW(file_utils::read_file(nonexistent_file), std::runtime_error);
}

TEST_F(FileUtilsTest, ReadFileHandlesEmptyFile) {
    std::string empty_file = test_dir + "empty.txt";
    std::ofstream file(empty_file);
    file.close();

    std::string content = file_utils::read_file(empty_file);
    EXPECT_TRUE(content.empty());

    std::remove(empty_file.c_str());
}

TEST_F(FileUtilsTest, ReadFileHandlesLargeFile) {
    std::string large_file = test_dir + "large.txt";
    std::ofstream file(large_file);
    
    // Write 100KB of data
    const int size = 100 * 1024;
    std::string large_content(size, 'A');
    file << large_content;
    file.close();

    std::string content = file_utils::read_file(large_file);
    EXPECT_EQ(content.size(), size);
    EXPECT_EQ(content, large_content);

    std::remove(large_file.c_str());
}

TEST_F(FileUtilsTest, FormatCsvAsTableFormatsCorrectly) {
    // Prepare test data
    std::vector<std::string> column_names = {"ID", "Name", "Value"};
    std::vector<std::vector<std::string>> rows = {
        {"1", "Alice", "100"},
        {"2", "Bob", "200"},
        {"3", "Charlie", "300"}
    };

    // Format as table
    std::string table = file_utils::format_csv_as_table(column_names, rows);

    // Verify table structure
    // Should have:
    // - 5 lines (top border, header, separator, 3 data rows, bottom border)
    // - Each line should start with + or |
    // - Header and data rows should have pipe separators (|)
    // - Border lines should have plus separators (+)

    std::istringstream iss(table);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(iss, line)) {
        lines.push_back(line);
    }

    // Verify number of lines (should be 7: top border, header, separator, 3 data rows, bottom border)
    EXPECT_EQ(lines.size(), 7);
    
    // Check first line (top border)
    EXPECT_TRUE(lines[0].find('+') == 0);
    EXPECT_TRUE(lines[0].find('+', 1) != std::string::npos);
    EXPECT_TRUE(lines[0].find('-') != std::string::npos);
    
    // Check header line
    EXPECT_TRUE(lines[1].find('|') == 0);
    EXPECT_TRUE(lines[1].find("ID") != std::string::npos);
    EXPECT_TRUE(lines[1].find("Name") != std::string::npos);
    EXPECT_TRUE(lines[1].find("Value") != std::string::npos);
    
    // Check separator line
    EXPECT_TRUE(lines[2].find('+') == 0);
    EXPECT_TRUE(lines[2].find('-') != std::string::npos);
    
    // Check data rows
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(lines[3 + i].find('|') == 0);
        EXPECT_TRUE(lines[3 + i].find(rows[i][0]) != std::string::npos);
        EXPECT_TRUE(lines[3 + i].find(rows[i][1]) != std::string::npos);
        EXPECT_TRUE(lines[3 + i].find(rows[i][2]) != std::string::npos);
    }
    
    // Check bottom border
    EXPECT_TRUE(lines[6].find('+') == 0);
    EXPECT_TRUE(lines[6].find('-') != std::string::npos);
}

TEST_F(FileUtilsTest, FormatCsvAsTableHandlesEmptyData) {
    // Empty column names and rows
    std::vector<std::string> empty_columns;
    std::vector<std::vector<std::string>> empty_rows;
    
    // Should return a simple message or minimal table
    std::string table = file_utils::format_csv_as_table(empty_columns, empty_rows);
    EXPECT_FALSE(table.empty());
}

TEST_F(FileUtilsTest, FormatCsvAsTableHandlesVaryingRowLengths) {
    // Columns
    std::vector<std::string> column_names = {"ID", "Name", "Value"};
    
    // Rows with varying lengths
    std::vector<std::vector<std::string>> rows = {
        {"1", "Alice"},                // Missing last column
        {"2", "Bob", "200", "Extra"},  // Extra column
        {"3"}                          // Missing last two columns
    };
    
    // Format as table
    std::string table = file_utils::format_csv_as_table(column_names, rows);
    
    // Basic validation
    std::istringstream iss(table);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(iss, line)) {
        lines.push_back(line);
    }
    
    // There should still be 7 lines
    EXPECT_EQ(lines.size(), 7);
    
    // Check that the rows contain the right data
    EXPECT_TRUE(lines[3].find("Alice") != std::string::npos);
    EXPECT_TRUE(lines[4].find("Bob") != std::string::npos);
    EXPECT_TRUE(lines[5].find("3") != std::string::npos);
}

TEST_F(FileUtilsTest, FormatCsvAsTableHandlesLongValues) {
    // Prepare test data with some very long values
    std::vector<std::string> column_names = {"ID", "Description", "Value"};
    std::vector<std::vector<std::string>> rows = {
        {"1", "This is a very long description that spans multiple characters and should be handled properly by the formatting function", "100"},
        {"2", "Short desc", "200"},
        {"3", "Another very long description to ensure consistent formatting across all rows even when column widths vary significantly", "300"}
    };
    
    // Format as table
    std::string table = file_utils::format_csv_as_table(column_names, rows);
    
    // Basic validation
    std::istringstream iss(table);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(iss, line)) {
        lines.push_back(line);
    }
    
    // Verify number of lines
    EXPECT_EQ(lines.size(), 7);
    
    // Each row should have consistent width
    size_t width = lines[0].length();
    for (const auto& line : lines) {
        EXPECT_EQ(line.length(), width);
    }
}

TEST_F(FileUtilsTest, FormatCsvAsTableHandlesSpecialCharacters) {
    // Test with special characters
    std::vector<std::string> column_names = {"ID", "Special", "Value"};
    std::vector<std::vector<std::string>> rows = {
        {"1", "Line\nBreak", "100"},
        {"2", "Tab\tCharacter", "200"},
        {"3", "Unicode: ééé", "300"}
    };
    
    // Format as table
    std::string table = file_utils::format_csv_as_table(column_names, rows);
    
    // At minimum it shouldn't crash
    EXPECT_FALSE(table.empty());
    
    // Basic table structure validation
    std::istringstream iss(table);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(iss, line)) {
        lines.push_back(line);
    }
    
    // Should still form a proper table
    EXPECT_GE(lines.size(), 7);
}
