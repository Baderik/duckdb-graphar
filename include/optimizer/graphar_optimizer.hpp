#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

using replace_col_map = unordered_map<string, ColumnBinding>;
using using_col_set = unordered_map<idx_t, unordered_set<idx_t>>;

class GraphArOptimizerExtension : public OptimizerExtension {
public:
    GraphArOptimizerExtension();
};

}  // namespace duckdb