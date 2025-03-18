#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/named_parameter_map.hpp"

#include "graphar/graph_info.h"


namespace duckdb {

class VerticesBindData final : public TableFunctionData {
public:
	string file_path;
	string v_type;
	std::vector<std::string> prop_names;
	std::vector<std::string> prop_types;
//	const shared_ptr<graphar::GraphInfo> graph_info;
//	const shared_ptr<graphar::GraphInfo> vertex_info;
};

struct VerticesGlobalState {
public:
	VerticesGlobalState(ClientContext &context, VerticesBindData bind_data);

public:
	string file_path;
	string v_type;
	std::vector<std::string> prop_names;
	std::vector<std::string> prop_types;
	idx_t chunk_count = 0;
};

struct VerticesGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	VerticesGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input);

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

public:
	VerticesGlobalState state;
};


struct ReadVertices {
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
	static void Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output);
	static void Register(DatabaseInstance &db);
	static TableFunction GetFunction();
};
}