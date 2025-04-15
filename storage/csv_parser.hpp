// CSV parsing utilities
#pragma once

#include "column_store.hpp"
#include <string>
#include <sstream>

namespace csv {

ColumnStore parse_csv(const std::string& csv_data);

} // namespace csv
