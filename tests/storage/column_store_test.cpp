#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "storage/column_store.hpp"
#include "tests/common/test_utils.hpp"

#include <string>
#include <vector>
#include <limits>
#include <stdexcept>

using namespace testing;
using namespace csv_test;

class ColumnStoreTest : public ::testing::Test {
protected:
    ColumnStore store;
    
    void SetUp() override {
        // Setup a basic column store for testing
        store.column_names = {"ID", "Value1", "Value2"};
        
        // Add some data
        store.columns["ID"] = {"1", "2", "3"};
        store.columns["Value1"] = {"10", "20", "30"};
        store.columns["Value2"] = {"100", "200", "300"};
    }
};

// Tests for compute_sum function
TEST_F(ColumnStoreTest, ComputeSumReturnsCorrectSum) {
    // Sum of Value1 column: 10 + 20 + 30 = 60
    EXPECT_DOUBLE_EQ(compute_sum(store, "Value1"), 60.0);
    
    // Sum of Value2 column: 100 + 200 + 300 = 600
    EXPECT_DOUBLE_EQ(compute_sum(store, "Value2"), 600.0);
}

TEST_F(ColumnStoreTest, ComputeSumHandlesEmptyColumn) {
    ColumnStore empty_store;
    empty_store.column_names = {"Empty"};
    empty_store.columns["Empty"] = {};
    
    EXPECT_DOUBLE_EQ(compute_sum(empty_store, "Empty"), 0.0);
}

TEST_F(ColumnStoreTest, ComputeSumHandlesNonNumericValues) {
    // Add a column with non-numeric values
    store.column_names.push_back("Text");
    store.columns["Text"] = {"abc", "def", "ghi"};
    
    // Should throw an exception or return a specific error value
    EXPECT_THROW(compute_sum(store, "Text"), std::invalid_argument);
}

TEST_F(ColumnStoreTest, ComputeSumHandlesNonExistentColumn) {
    // Sum of a column that doesn't exist
    EXPECT_THROW(compute_sum(store, "NonExistent"), std::out_of_range);
}

TEST_F(ColumnStoreTest, ComputeSumHandlesLargeValues) {
    // Create a store with large values
    ColumnStore large_store;
    large_store.column_names = {"Large"};
    
    // Values close to numeric limits
    large_store.columns["Large"] = {
        "1000000000", // 1 billion
        "2000000000", // 2 billion
        "3000000000"  // 3 billion
    };
    
    // Sum: 6 billion
    EXPECT_DOUBLE_EQ(compute_sum(large_store, "Large"), 6000000000.0);
}

TEST_F(ColumnStoreTest, ComputeSumHandlesMixedValidAndInvalidValues) {
    // Create a store with mixed valid and invalid values
    ColumnStore mixed_store;
    mixed_store.column_names = {"Mixed"};
    mixed_store.columns["Mixed"] = {"10", "invalid", "30", "40"};
    
    // Should throw or handle appropriately
    EXPECT_THROW(compute_sum(mixed_store, "Mixed"), std::invalid_argument);
}

// Tests for compute_average function
TEST_F(ColumnStoreTest, ComputeAverageReturnsCorrectAverage) {
    // Average of Value1 column: (10 + 20 + 30) / 3 = 20
    EXPECT_DOUBLE_EQ(compute_average(store, "Value1"), 20.0);
    
    // Average of Value2 column: (100 + 200 + 300) / 3 = 200
    EXPECT_DOUBLE_EQ(compute_average(store, "Value2"), 200.0);
}

TEST_F(ColumnStoreTest, ComputeAverageHandlesEmptyColumn) {
    ColumnStore empty_store;
    empty_store.column_names = {"Empty"};
    empty_store.columns["Empty"] = {};
    
    // Average of empty column should throw or return a specific value
    EXPECT_THROW(compute_average(empty_store, "Empty"), std::invalid_argument);
}

TEST_F(ColumnStoreTest, ComputeAverageHandlesNonNumericValues) {
    // Add a column with non-numeric values
    store.column_names.push_back("Text");
    store.columns["Text"] = {"abc", "def", "ghi"};
    
    // Should throw an exception or return a specific error value
    EXPECT_THROW(compute_average(store, "Text"), std::invalid_argument);
}

TEST_F(ColumnStoreTest, ComputeAverageHandlesNonExistentColumn) {
    // Average of a column that doesn't exist
    EXPECT_THROW(compute_average(store, "NonExistent"), std::out_of_range);
}

// Tests for insert_row function
TEST_F(ColumnStoreTest, InsertRowAddsNewRow) {
    // Initial size
    size_t initial_size = store.columns["ID"].size();
    
    // Insert a new row
    insert_row(store, {"4", "40", "400"});
    
    // Check size increased
    EXPECT_EQ(store.columns["ID"].size(), initial_size + 1);
    
    // Check values
    EXPECT_EQ(store.columns["ID"].back(), "4");
    EXPECT_EQ(store.columns["Value1"].back(), "40");
    EXPECT_EQ(store.columns["Value2"].back(), "400");
}

TEST_F(ColumnStoreTest, InsertRowHandlesFewerValuesThanColumns) {
    // Insert a row with fewer values than columns
    insert_row(store, {"5", "50"});
    
    // Check values (third column should be empty or default)
    EXPECT_EQ(store.columns["ID"].back(), "5");
    EXPECT_EQ(store.columns["Value1"].back(), "50");
    EXPECT_EQ(store.columns["Value2"].back(), ""); // Empty or default value
}

TEST_F(ColumnStoreTest, InsertRowHandlesMoreValuesThanColumns) {
    // Insert a row with more values than columns
    insert_row(store, {"6", "60", "600", "extra"});
    
    // Check values (should ignore extra values)
    EXPECT_EQ(store.columns["ID"].back(), "6");
    EXPECT_EQ(store.columns["Value1"].back(), "60");
    EXPECT_EQ(store.columns["Value2"].back(), "600");
    
    // No extra column should be created
    EXPECT_EQ(store.column_names.size(), 3);
}

TEST_F(ColumnStoreTest, InsertRowIntoEmptyStore) {
    // Create an empty store
    ColumnStore empty_store;
    empty_store.column_names = {"Col1", "Col2"};
    empty_store.columns["Col1"] = {};
    empty_store.columns["Col2"] = {};
    
    // Insert a row
    insert_row(empty_store, {"value1", "value2"});
    
    // Check values
    EXPECT_EQ(empty_store.columns["Col1"].size(), 1);
    EXPECT_EQ(empty_store.columns["Col1"][0], "value1");
    EXPECT_EQ(empty_store.columns["Col2"][0], "value2");
}

// Tests for delete_row function
TEST_F(ColumnStoreTest, DeleteRowRemovesRow) {
    // Initial size
    size_t initial_size = store.columns["ID"].size();
    
    // Values to check for removal
    std::string id_to_remove = store.columns["ID"][1]; // Second row
    
    // Delete the second row
    delete_row(store, 1);
    
    // Check size decreased
    EXPECT_EQ(store.columns["ID"].size(), initial_size - 1);
    
    // Check the row was removed
    EXPECT_THAT(store.columns["ID"], Not(Contains(id_to_remove)));
}

TEST_F(ColumnStoreTest, DeleteRowHandlesFirstRow) {
    // Values to check for removal
    std::string id_to_remove = store.columns["ID"][0]; // First row
    
    // Delete the first row
    delete_row(store, 0);
    
    // Check the first row was removed
    EXPECT_THAT(store.columns["ID"], Not(Contains(id_to_remove)));
    
    // Check the new first row
    EXPECT_EQ(store.columns["ID"][0], "2");
}

TEST_F(ColumnStoreTest, DeleteRowHandlesLastRow) {
    // Initial size
    size_t initial_size = store.columns["ID"].size();
    
    // Values to check for removal
    std::string id_to_remove = store.columns["ID"][initial_size - 1]; // Last row
    
    // Delete the last row
    delete_row(store, initial_size - 1);
    
    // Check the last row was removed
    EXPECT_THAT(store.columns["ID"], Not(Contains(id_to_remove)));
    
    // Check size
    EXPECT_EQ(store.columns["ID"].size(), initial_size - 1);
}

TEST_F(ColumnStoreTest, DeleteRowHandlesInvalidIndex) {
    // Delete row with invalid index (negative)
    EXPECT_THROW(delete_row(store, -1), std::out_of_range);
    
    // Delete row with invalid index (too large)
    EXPECT_THROW(delete_row(store, store.columns["ID"].size()), std::out_of_range);
}

TEST_F(ColumnStoreTest, DeleteRowFromEmptyStore) {
    // Create an empty store
    ColumnStore empty_store;
    empty_store.column_names = {"Col1"};
    empty_store.columns["Col1"] = {};
    
    // Delete from empty store should throw
    EXPECT_THROW(delete_row(empty_store, 0), std::out_of_range);
}

// Stress test with many operations
TEST_F(ColumnStoreTest, StressTestManyOperations) {
    // Start with empty store
    ColumnStore stress_store;
    stress_store.column_names = {"ID", "Value"};
    stress_store.columns["ID"] = {};
    stress_store.columns["Value"] = {};
    
    // Insert many rows
    const int NUM_ROWS = 1000;
    for (int i = 0; i < NUM_ROWS; i++) {
        insert_row(stress_store, {std::to_string(i), std::to_string(i * 10)});
    }
    
    // Check all rows were inserted
    EXPECT_EQ(stress_store.columns["ID"].size(), NUM_ROWS);
    
    // Delete some rows (every 100th row)
    for (int i = 0; i < 10; i++) {
        delete_row(stress_store, i * 100);
    }
    
    // Check the right number of rows remain
    EXPECT_EQ(stress_store.columns["ID"].size(), NUM_ROWS - 10);
    
    // Check sum is correct
    // Sum should be 10 * (0 + 1 + 2 + ... + 999) - 10 * (0 + 100 + 200 + ... + 900)
    // = 10 * 499500 - 10 * 4500 = 4995000 - 45000 = 4950000
    EXPECT_DOUBLE_EQ(compute_sum(stress_store, "Value"), 4950000.0);
    
    // Check average is correct
    EXPECT_NEAR(compute_average(stress_store, "Value"), 4950000.0 / (NUM_ROWS - 10), 0.01);
}
