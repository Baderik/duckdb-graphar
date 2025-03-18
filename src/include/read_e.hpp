#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/named_parameter_map.hpp"

#include "graphar/graph_info.h"


namespace duckdb {

class EdgesBindData final : public TableFunctionData {
public:
	string file_path;
	string e_type;
	string src_type;
	string dst_type;
	std::vector<std::string> prop_names;
	std::vector<std::string> prop_types;
//	const shared_ptr<graphar::GraphInfo> graph_info;
//	const shared_ptr<graphar::GraphInfo> edge_info;
};

struct EdgesGlobalState {
public:
	EdgesGlobalState(ClientContext &context, EdgesBindData bind_data);

public:
	string file_path;
	string e_type;
	string src_type;
	string dst_type;
	std::vector<std::string> prop_names;
	std::vector<std::string> prop_types;
	idx_t chunk_count = 0;
};

struct EdgesGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	EdgesGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input);

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

public:
	EdgesGlobalState state;
};


struct ReadEdges {
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
	static void Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output);
	static void Register(DatabaseInstance &db);
	static TableFunction GetFunction();
};
}