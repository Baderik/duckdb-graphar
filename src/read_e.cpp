#include "read_e.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/main/extension_util.hpp"

#include "graphar/api/high_level_reader.h"
#include "func.hpp"


namespace duckdb {
EdgesGlobalState::EdgesGlobalState(ClientContext &context, EdgesBindData bind_data) :
      file_path(bind_data.file_path),
      e_type(bind_data.e_type),
      src_type(bind_data.src_type),
      dst_type(bind_data.dst_type),
      prop_names(bind_data.prop_names),
      prop_types(bind_data.prop_types),
      chunk_count(0) {}

EdgesGlobalTableFunctionState::EdgesGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input) : state(context, input.bind_data->Cast<EdgesBindData>()) {
}

unique_ptr<GlobalTableFunctionState> EdgesGlobalTableFunctionState::Init(ClientContext &context, TableFunctionInitInput &input) {
	// Initialize any global state here
	std::cout << "Global state init" << std::endl;
	if (input.filters) {
		for (auto &filter : input.filters->filters) {
			std::cout << "filter " << filter.first << ' ' <<  (int)(filter.second->filter_type) << std::endl;
		}
	} else {
		std::cout << "no filters" << std::endl;
	}

	return make_uniq<EdgesGlobalTableFunctionState>(context, input);
}

//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadEdges::Bind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	std::cout << "input size " << input.inputs.size() << std::endl;
	std::cout << "input named " << input.named_parameters.size() << std::endl;

	std::cout << "table types:" << std::endl;
	for (auto el : input.input_table_types) {
		std::cout << '<' <<  (int)(el.id()) << '>' << std::endl;
	}
	std::cout << "table names:" << std::endl;
	for (auto el : input.input_table_names) {
		std::cout << '<' <<  el << '>' << std::endl;
	}

	const auto file_path = StringValue::Get(input.inputs[0]);
	const std::string e_type = StringValue::Get(input.named_parameters.at("type"));
	const std::string src_type = StringValue::Get(input.named_parameters.at("src"));
	const std::string dst_type = StringValue::Get(input.named_parameters.at("dst"));
	std::cout << "Get type " << e_type << " From " << src_type << " To " << dst_type << std::endl;
	auto bind_data = make_uniq<EdgesBindData>();
	auto graph_info = graphar::GraphInfo::Load(file_path).value();



	auto edge_info = graph_info->GetEdgeInfo(src_type, e_type, dst_type);

	if (!edge_info) {
		throw BinderException("No found edge this types");
	}

	for (auto pg : edge_info->GetPropertyGroups()) {
		for (auto p : pg->GetProperties()) {
			return_types.push_back(GrapharFunctions::grapharT2duckT(p.type->ToTypeName()));
			names.push_back(p.name);
			bind_data->prop_names.push_back(p.name);
			bind_data->prop_types.push_back(p.type->ToTypeName());
		}
	}

	return_types.push_back(LogicalTypeId::BIGINT);
	names.push_back("srcGrapharId");
	return_types.push_back(LogicalTypeId::BIGINT);
	names.push_back("dstGrapharId");


//	bind_data->graph_info = graph_info;
	bind_data->file_path = file_path;
	bind_data->e_type = e_type;
	bind_data->src_type = src_type;
	bind_data->dst_type = dst_type;
//	bind_data->edge_info = edge_info;
	std::cout << "Success bind" << std::endl;
	return std::move(bind_data);
}

//
// Execute
//

inline void ReadEdges::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {


	EdgesGlobalState &gstate = input.global_state->Cast<EdgesGlobalTableFunctionState>().state;
	if (gstate.chunk_count != 0) {
		output.SetCapacity(0);
		output.SetCardinality(0);
		return;
	}
	auto graph_info = graphar::GraphInfo::Load(gstate.file_path).value();
	auto maybe_edges = graphar::EdgesCollection::Make(graph_info, gstate.src_type, gstate.e_type, gstate.dst_type, graphar::AdjListType::ordered_by_source);
	if (maybe_edges.has_error()) {
		throw BinderException("(Execute) No found edge this type");
	}
	auto edges = maybe_edges.value();

	int row_count = edges->size();
	output.SetCapacity(row_count);
	output.SetCardinality(row_count);

	int i = 0, prop_number = gstate.prop_names.size();
	std::cout << "begin iter" << std::endl;
//	std::cout << "prop_count: " << prop_number << ' ' << gstate.prop_types.size() << std::endl;
	for (auto it = edges->begin(); it != edges->end(); ++it, ++i) {
		// access data through iterator directly
//		std::cout << "iter " << i << std::endl;
		for (int prop_i = 0; prop_i < prop_number; ++prop_i) {
			auto type_name = gstate.prop_types.at(prop_i);
//			std::cout << "prop_name " << gstate.prop_names[prop_i] << " " << type_name << std::endl;

			if (type_name == "bool") {
				output.SetValue(prop_i, i, it.property<bool>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "int32") {
				output.SetValue(prop_i, i, it.property<int32_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "int64") {
				output.SetValue(prop_i, i, it.property<int64_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "float") {
				output.SetValue(prop_i, i, it.property<float_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "double") {
				output.SetValue(prop_i, i, it.property<double_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "string") {
				output.SetValue(prop_i, i, it.property<std::string>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "date") {
				output.SetValue(prop_i, i, it.property<std::string>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "timestamp") {
				output.SetValue(prop_i, i, it.property<std::string>(gstate.prop_names.at(prop_i)).value());
			}
		}

		output.data[prop_number].SetValue(i, it.source()); // TODO: To real id
		output.data[prop_number + 1].SetValue(i, it.destination());

//		std::cout << it.id() << ", id=" << it.property<int64_t>("id").value()
//		          << ", firstName=" << it.property<std::string>("firstName").value()
//		          << "; ";
	}
	gstate.chunk_count ++;

	// Example: Generate some sample data
//	if (gstate.chunk_count == 0) {
//		auto graph_info = graphar::GraphInfo::Load(gstate.bind_data.file_path).value();
//		auto edges = graph_info->GetEdgeInfos();
//		row_count = edges.size();
//		output.SetCardinality(row_count);
//
//		for (int i = 0; i < row_count; ++i) {
//			output.data[0].SetValue(i, edges[i]->GetSrcType());
//			output.data[1].SetValue(i, edges[i]->GetEdgeType());
//			output.data[2].SetValue(i, edges[i]->GetDstType());
//		}
//		gstate.chunk_count ++;
//	}
}

//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
TableFunction ReadEdges::GetFunction() {

	TableFunction read_edges("read_edges", {LogicalType::VARCHAR}, Execute, Bind);
	read_edges.init_global = EdgesGlobalTableFunctionState::Init;

	read_edges.named_parameters["type"] = LogicalType::VARCHAR;
	read_edges.named_parameters["src"] = LogicalType::VARCHAR;
	read_edges.named_parameters["dst"] = LogicalType::VARCHAR;

	return read_edges;
}

void ReadEdges::Register(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, GetFunction());
//	db.config.replacement_scans.emplace_back(XLSXReplacementScan);
}

} // namespace duckdb