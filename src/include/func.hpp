#pragma once

#include "duckdb/common/types.hpp"
#include "graphar/types.h"


namespace duckdb {

struct GrapharFunctions {
	static LogicalTypeId grapharT2duckT(const std::string &name);
};

}
