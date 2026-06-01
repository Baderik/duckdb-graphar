#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class GraphArOptimizerExtension : public OptimizerExtension {
public:
	GraphArOptimizerExtension();
};

} // namespace duckdb