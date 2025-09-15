#include "functions/table/hop_thread.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/main/extension_util.hpp>

#include <graphar/api/high_level_reader.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> TestF::Bind(ClientContext& context, TableFunctionBindInput& input,
                                     vector<LogicalType>& return_types, vector<string>& names) {
    const auto file_path = StringValue::Get(input.inputs[0]);
    const auto file_path_2 = StringValue::Get(input.inputs[1]);

    auto bind_data = std::make_unique<TestBindData>(file_path, file_path_2);

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(SRC_GID_COLUMN);
    return_types.push_back(LogicalType::BIGINT);
    names.push_back(DST_GID_COLUMN);

    return std::move(bind_data);
}

unique_ptr<FunctionData> TwoHopParquet::Bind(ClientContext& context, TableFunctionBindInput& input,
                                             vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopParquet::Bind");

    const auto edge_info_path = StringValue::Get(input.inputs[0]);
    const int64_t vid = IntegerValue::Get(input.named_parameters.at("vid"));

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(SRC_GID_COLUMN);
    return_types.push_back(LogicalType::BIGINT);
    names.push_back(DST_GID_COLUMN);

    return std::move(std::make_unique<TwoHopParquetBindData>(edge_info_path, vid));
}
//-------------------------------------------------------------------
// State Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> TestGlobalTableFunctionState::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("Test::GlobalStateInit");
    auto bind_data = input.bind_data->Cast<TestBindData>();

    return std::make_unique<TestGlobalTableFunctionState>(context, bind_data);
}

unique_ptr<LocalTableFunctionState> TestLocalTableFunctionState::Init(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state) {
    DUCKDB_GRAPHAR_LOG_TRACE("Test::LocalStateInit");
    auto local_state = std::make_unique<TestLocalTableFunctionState>();
    return std::move(local_state);
}

unique_ptr<GlobalTableFunctionState> TwoHopParquetGlobalTableFunctionState::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopParquet::GlobalInit");
    auto bind_data = input.bind_data->Cast<TwoHopParquetBindData>();
    std::unique_ptr<TwoHopParquetGlobalTableFunctionState> global_state = std::make_unique<TwoHopParquetGlobalTableFunctionState>(context, bind_data);
    auto &state = global_state->GetState();
    std::string yaml_content = GetYamlContent(bind_data.edge_info_path);
    auto edge_info = graphar::EdgeInfo::Load(yaml_content).value();
    if (!edge_info) {
        throw BinderException("No found edge this type");
    }
    const std::string prefix = GetDirectory(bind_data.edge_info_path);
    std::shared_ptr<OffsetReader> offset_reader = std::make_shared<OffsetReader>(edge_info, prefix, graphar::AdjListType::ordered_by_source);
    LowEdgeReaderByVertex one_hop_reader = LowEdgeReaderByVertex(edge_info,
                                                                 prefix,
                                                                 graphar::AdjListType::ordered_by_source,
                                                                 offset_reader);
    one_hop_reader.SetVertex(bind_data.vid);

    std::unique_ptr<Connection> conn = std::make_unique<Connection>(*context.db);
    one_hop_reader.start(std::move(conn));

    for (std::unique_ptr<DataChunk> data = std::move(one_hop_reader.read()); data != nullptr; data = std::move(one_hop_reader.read())) {
        for (int i = 0; i < data->size(); ++i) {
            std::unique_ptr<LowEdgeReaderByVertex> reader = std::make_unique<LowEdgeReaderByVertex>(edge_info,
                                                                                                    prefix,
                                                                                                    graphar::AdjListType::ordered_by_source,
                                                                                                    offset_reader);
            reader->SetVertex(data->GetValue(1, i).GetValue<int64_t>());
            state.vertex_readers.push(std::move(reader));

        }
    }

    return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> TwoHopParquetLocalTableFunctionState::Init(ExecutionContext &context,
                                                                               TableFunctionInitInput &input,
                                                                               GlobalTableFunctionState *global_state) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopParquet::LocalStateInit");
    std::unique_ptr<TwoHopParquetLocalTableFunctionState> local_state = std::make_unique<TwoHopParquetLocalTableFunctionState>();
    return std::move(local_state);
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void TestF::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    TestLocalTableFunctionState& local_state = input.local_state->Cast<TestLocalTableFunctionState>();
    if (local_state.first) {
        output.SetCapacity(1);
        output.SetCardinality(1);
        local_state.first = false;
        //        DUCKDB_GRAPHAR_LOG_INFO("Test::Execute");
        output.SetValue(0, 0, 1);
        output.SetValue(1, 0, 1);
        return;
    }
}

void TwoHopParquet::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopParquet::Execute")
    TwoHopParquetGlobalState& gstate = input.global_state->Cast<TwoHopParquetGlobalTableFunctionState>().GetState();

    std::unique_ptr<LowEdgeReaderByVertex> reader;
    while (!gstate.vertex_readers.empty()) {
        {
            std::lock_guard<std::mutex> lock(gstate.vertex_readers_mutex);
            reader = std::move(gstate.vertex_readers.front());
            gstate.vertex_readers.pop();
        }
        if (!reader->started()) {
            std::unique_ptr<Connection> conn = std::make_unique<Connection>(*context.db);
            reader->start(std::move(conn));
        }
        std::unique_ptr<DataChunk> data = std::move(reader->read());

        if (data != nullptr) {
            output.Reference(*data);
            {
                std::lock_guard<std::mutex> lock(gstate.vertex_readers_mutex);
                gstate.vertex_readers.push(std::move(reader));
            }
            return;
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG("Vertex finished")
        }
    }
    output.SetCardinality(0);
}
//-------------------------------------------------------------------
// Function
//-------------------------------------------------------------------
TableFunction TestF::GetFunction() {
    TableFunction test_f("test_f", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute, Bind,
                         TestGlobalTableFunctionState::Init, TestLocalTableFunctionState::Init);

    return test_f;
}

TableFunction TwoHopParquet::GetFunction() {
    TableFunction read_edges("two_hop_parquet", {LogicalType::VARCHAR}, Execute, Bind,
                             TwoHopParquetGlobalTableFunctionState::Init, TwoHopParquetLocalTableFunctionState::Init);
    read_edges.named_parameters["vid"] = LogicalType::INTEGER;

    return read_edges;
}

void TestF::Register(DatabaseInstance& db) { ExtensionUtil::RegisterFunction(db, GetFunction()); }

void TwoHopParquet::Register(DatabaseInstance& db) { ExtensionUtil::RegisterFunction(db, GetFunction()); }
}