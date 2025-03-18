#include "read_v.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/main/extension_util.hpp"

#include "graphar/api/high_level_reader.h"

#include "func.hpp"

namespace duckdb {
VerticesGlobalState::VerticesGlobalState(ClientContext &context, VerticesBindData bind_data) :
      file_path(bind_data.file_path), v_type(bind_data.v_type), prop_names(bind_data.prop_names), prop_types(bind_data.prop_types), chunk_count(0) {}

VerticesGlobalTableFunctionState::VerticesGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input) : state(context, input.bind_data->Cast<VerticesBindData>()) {
}

unique_ptr<GlobalTableFunctionState> VerticesGlobalTableFunctionState::Init(ClientContext &context, TableFunctionInitInput &input) {
	// Initialize any global state here
	return make_uniq<VerticesGlobalTableFunctionState>(context, input);
}


//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadVertices::Bind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	const auto file_path = StringValue::Get(input.inputs[0]);
	const std::string v_type = StringValue::Get(input.named_parameters.at("type"));
	std::cout << "Get type " << v_type << std::endl;
	auto bind_data = make_uniq<VerticesBindData>();
	auto graph_info = graphar::GraphInfo::Load(file_path).value();

	auto vertex_info = graph_info->GetVertexInfo(v_type);

	if (!vertex_info) {
		throw BinderException("No found vertex this type");
	}

	for (auto pg : vertex_info->GetPropertyGroups()) {
		for (auto p : pg->GetProperties()) {
			return_types.push_back(GrapharFunctions::grapharT2duckT(p.type->ToTypeName()));
			names.push_back(p.name);
			bind_data->prop_names.push_back(p.name);
			bind_data->prop_types.push_back(p.type->ToTypeName());
		}
	}

	return_types.push_back(LogicalTypeId::BIGINT);
	names.push_back("grapharId");


//	bind_data->graph_info = graph_info;
	bind_data->file_path = file_path;
	bind_data->v_type = v_type;
//	bind_data->vertex_info = vertex_info;
	std::cout << "Success bind" << std::endl;
	return std::move(bind_data);
}

//
// Execute
//

inline void ReadVertices::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	VerticesGlobalState &gstate = input.global_state->Cast<VerticesGlobalTableFunctionState>().state;
	if (gstate.chunk_count != 0) {
		output.SetCapacity(0);
		output.SetCardinality(0);
		return;
	}
	auto graph_info = graphar::GraphInfo::Load(gstate.file_path).value();
	auto maybe_vertices = graphar::VerticesCollection::Make(graph_info, gstate.v_type);
	if (maybe_vertices.has_error()) {
		throw BinderException("(Execute) No found vertex this type");
	}
	auto vertices = maybe_vertices.value();

	int row_count = vertices->size();
	output.SetCapacity(row_count);
	output.SetCardinality(row_count);

	int i = 0, prop_number = gstate.prop_names.size();
	std::cout << "begin iter" << std::endl;
//	std::cout << "prop_count: " << prop_number << ' ' << gstate.prop_types.size() << std::endl;
	for (auto it = vertices->begin(); it != vertices->end(); ++it, ++i) {
		// access data through iterator directly
//		std::cout << "iter " << i << std::endl;
		for (int prop_i = 0; prop_i < prop_number; ++prop_i) {
			auto type_name = gstate.prop_types.at(prop_i);
//			std::cout << "prop_name " << gstate.prop_names[prop_i] << " " << type_name << std::endl;

			if (type_name == "bool") {
				output.data[prop_i].SetValue(i, it.property<bool>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "int32") {
				output.data[prop_i].SetValue(i, it.property<int32_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "int64") {
				output.data[prop_i].SetValue(i, it.property<int64_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "float") {
				output.data[prop_i].SetValue(i, it.property<float_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "double") {
				output.data[prop_i].SetValue(i, it.property<double_t>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "string") {
				output.data[prop_i].SetValue(i, it.property<std::string>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "date") {
				output.data[prop_i].SetValue(i, it.property<std::string>(gstate.prop_names.at(prop_i)).value());
			} else if (type_name == "timestamp") {
				output.data[prop_i].SetValue(i, it.property<std::string>(gstate.prop_names.at(prop_i)).value());
			}
		}

		output.data[prop_number].SetValue(i, i); // TODO: To real id

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
TableFunction ReadVertices::GetFunction() {

	TableFunction read_vertices("read_vertices", {LogicalType::VARCHAR}, Execute, Bind);
	read_vertices.init_global = VerticesGlobalTableFunctionState::Init;

	read_vertices.named_parameters["type"] = LogicalType::VARCHAR;

	return read_vertices;
}

void ReadVertices::Register(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, GetFunction());
//	db.config.replacement_scans.emplace_back(XLSXReplacementScan);
}

} // namespace duckdb