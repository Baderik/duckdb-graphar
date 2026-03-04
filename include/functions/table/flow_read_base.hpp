#pragma once

#include "functions/table/read_base.hpp"
#include "readers/base_reader.hpp"
#include "readers/duck_arrow_chunk_reader.hpp"
#include "readers/duck_chunk_reader.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"
#include "utils/type_info.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/expression.h>
#include <graphar/fwd.h>
#include <graphar/graph_info.h>
#include <graphar/reader_util.h>

#include <boost/lockfree/queue.hpp>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <variant>

namespace duckdb {

class FlowReadBaseGlobalTableFunctionState : public ReadBaseGlobalTableFunctionState {
private:
    boost::lockfree::queue<int> vertexes;
};

}  // namespace duckdb