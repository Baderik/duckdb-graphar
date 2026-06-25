#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

using replace_col_map = unordered_map<string, ColumnBinding>;

class GraphArOptimizerExtension : public OptimizerExtension {
public:
	GraphArOptimizerExtension();
};

} // namespace duckdb