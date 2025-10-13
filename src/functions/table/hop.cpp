#include "functions/table/hop.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/function/table_function.hpp>

#include <graphar/api/high_level_reader.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> TwoHop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                      vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    DUCKDB_GRAPHAR_LOG_TRACE("Hop::Bind");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const int64_t vid = IntegerValue::Get(input.named_parameters.at("vid"));

    DUCKDB_GRAPHAR_LOG_DEBUG("Load Edge Info");

    auto yaml_content = GetYamlContent(file_path);
    auto edge_info = graphar::EdgeInfo::Load(yaml_content).value();
    if (!edge_info) {
        throw BinderException("No found edge this type");
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Create BindData");

    const std::string prefix = GetDirectory(file_path);
    auto bind_data = make_uniq<TwoHopBindData>(edge_info, prefix, vid);

    DUCKDB_GRAPHAR_LOG_DEBUG("Set types and names");

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(SRC_GID_COLUMN);
    return_types.push_back(LogicalType::BIGINT);
    names.push_back(DST_GID_COLUMN);

    DUCKDB_GRAPHAR_LOG_DEBUG("Bind finish");
    if (time_logging) {
        t.print();
    }

    return bind_data;
}
unique_ptr<FunctionData> OneMoreHop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                          vector<LogicalType>& return_types, vector<string>& names) {
    return TwoHop::Bind(context, input, return_types, names);
}
unique_ptr<FunctionData> FastTwoHop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                          vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_DEBUG("FastTwoHop::Bind");
    return TwoHop::Bind(context, input, return_types, names);
}
//-------------------------------------------------------------------
// State Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> TwoHopGlobalTableFunctionState::Init(ClientContext& context,
                                                                          TableFunctionInitInput& input) {
    auto bind_data = input.bind_data->Cast<TwoHopBindData>();

    return make_uniq<TwoHopGlobalTableFunctionState>(context, bind_data);
}
unique_ptr<GlobalTableFunctionState> OneMoreHopGlobalTableFunctionState::Init(ClientContext& context,
                                                                              TableFunctionInitInput& input) {
    auto bind_data = input.bind_data->Cast<TwoHopBindData>();

    return make_uniq<OneMoreHopGlobalTableFunctionState>(context, bind_data);
}
unique_ptr<GlobalTableFunctionState> FastTwoHopGTFS::Init(ClientContext& context,
                                                          TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_DEBUG("FastTwoHop::GlobalTableFunctionState::Init");
    auto bind_data = input.bind_data->Cast<FastTwoHopBD>();

    std::unique_ptr<FastTwoHopGTFS> gtfs = make_uniq<FastTwoHopGTFS>(context, bind_data);
    FastTwoHopGS& state = gtfs->GetState();
    state.offset_reader = std::make_shared<OffsetReader>(bind_data.GetEdgeInfo(),
                                                         bind_data.GetPrefix(),
                                                         graphar::AdjListType::ordered_by_source);
    state.reader = std::make_unique<LowEdgeReaderByVertex>(bind_data.GetEdgeInfo(),
                                                           bind_data.GetPrefix(),
                                                           graphar::AdjListType::ordered_by_source,
                                                           state.offset_reader);
    DUCKDB_GRAPHAR_LOG_DEBUG("Reader started before " + to_string(state.reader->started()));
    state.reader->SetVertex(bind_data.GetVid());
    DUCKDB_GRAPHAR_LOG_DEBUG("Reader started after " + to_string(state.reader->started()));

    return std::move(gtfs);
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
inline void OneHopExecute(TwoHopGlobalState& state, DataChunk& output, const bool time_logging) {
    auto table = state.GetSrcReader().get(STANDARD_VECTOR_SIZE);

    output.SetCapacity(table->num_rows());
    output.SetCardinality(table->num_rows());

    int num_columns = table->num_columns();

    DUCKDB_GRAPHAR_LOG_DEBUG("OneHopExecute::iterations " + std::to_string(num_columns));

    for (int col_i = 0; col_i < num_columns; ++col_i) {
        auto column = table->column(col_i);
        int64_t row_offset = 0;
        for (const auto& chunk : column->chunks()) {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
            for (int64_t i = 0; i < int_array->length(); ++i) {
                output.SetValue(col_i, row_offset + i, int_array->Value(i));
                if (state.IsOneHop() && col_i == 1) {
                    state.AddHopId(int_array->Value(i));
                }
            }
            row_offset += int_array->length();
        }
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("OneHopExecute::Finish " + std::to_string(table->num_rows()));
}

inline int64_t OneMoreHopExecute(OneMoreHopGlobalState& state, DataChunk& output, const bool time_logging) {
    DUCKDB_GRAPHAR_LOG_DEBUG("OneMoreHopExecute::get " + std::to_string(STANDARD_VECTOR_SIZE));
    auto table = state.src_reader.get(STANDARD_VECTOR_SIZE);

    output.SetCapacity(table->num_rows());
    output.SetCardinality(table->num_rows());

    int num_columns = table->num_columns();

    DUCKDB_GRAPHAR_LOG_DEBUG("OneMoreHopExecute::iterations " + std::to_string(num_columns));
    std::vector<bool> valid(table->num_rows(), false);
    int64_t number_valid = 0;

    for (int col_i = num_columns - 1; col_i >= 0; --col_i) {
        auto column = table->column(col_i);
        int64_t row_offset = 0;
        for (const auto& chunk : column->chunks()) {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
            for (int64_t i = 0; i < int_array->length(); ++i) {
                if (state.one_hop) {
                    output.SetValue(col_i, row_offset + i, int_array->Value(i));
                } else {
                    if (col_i == 1) {
                        if (state.hop_ids.find(int_array->Value(i)) != state.hop_ids.end()) {
                            valid[row_offset + i] = true;
                            output.SetValue(col_i, row_offset + i, int_array->Value(i));
                            number_valid++;
                        }
                    } else {
                        if (valid[row_offset + i]) {
                            output.SetValue(col_i, row_offset + i, int_array->Value(i));
                        }
                    }
                }

                if (state.one_hop && col_i == 1) {
                    state.hop_ids.insert(int_array->Value(i));
                }

                row_offset += int_array->length();
            }
        }
    }

    output.SetCapacity(number_valid);
    output.SetCardinality(number_valid);

    DUCKDB_GRAPHAR_LOG_DEBUG("OneMoreHopExecute::Finish " + std::to_string(number_valid));
    return number_valid;
}

inline void TwoHop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Execute");

    DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::Execute");
    DUCKDB_GRAPHAR_LOG_DEBUG("Cast Global state");

    TwoHopGlobalState& gstate = input.global_state->Cast<TwoHopGlobalTableFunctionState>().GetState();

    if (gstate.GetSrcReader().finish()) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Reader finished");
        return;
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Begin iteration");
    OneHopExecute(gstate, output, time_logging);

    if (gstate.GetSrcReader().finish()) {
        if (gstate.IsOneHop()) {
            gstate.SetOneHop(false);
        }
        while (gstate.GetHopI() < gstate.GetHopIds().size() && gstate.GetSrcReader().finish()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Find next hop " + std::to_string(gstate.GetHopIds()[gstate.GetHopI()]));
            gstate.GetSrcReader().find_src(gstate.GetHopIds()[gstate.IncrementHopI()]);
        }
    }

    if (time_logging) {
        t.print();
    }
}

inline void OneMoreHop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Execute");

    DUCKDB_GRAPHAR_LOG_TRACE("OneMoreHop::Execute");
    DUCKDB_GRAPHAR_LOG_DEBUG("Cast Global state");

    OneMoreHopGlobalState& gstate = input.global_state->Cast<OneMoreHopGlobalTableFunctionState>().state;

    if (gstate.src_reader.finish()) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Reader finished");
        return;
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Begin iteration");
    int64_t row_count;
    bool is_one_hop;
    do {
        row_count = OneMoreHopExecute(gstate, output, time_logging);
        is_one_hop = gstate.one_hop;

        if (gstate.src_reader.finish()) {
            if (gstate.one_hop) {
                gstate.one_hop = false;
            }
            while (gstate.hop_i != gstate.hop_ids.end() && gstate.src_reader.finish()) {
                DUCKDB_GRAPHAR_LOG_DEBUG("Find next hop " + std::to_string(*gstate.hop_i));
                gstate.src_reader.find_src(*gstate.hop_i);
                ++gstate.hop_i;
            }
        }
    } while (row_count == 0 && !is_one_hop && !gstate.src_reader.finish());

    if (time_logging) {
        t.print();
    }
}

void FastTwoHop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_DEBUG("FastTwoHop::Execute");

    FastTwoHopGS& global_state = input.global_state->Cast<FastTwoHopGTFS>().GetState();

    DUCKDB_GRAPHAR_LOG_DEBUG("global state casted");
    std::unique_ptr<DataChunk> data;
    DUCKDB_GRAPHAR_LOG_DEBUG("Reader exist " + to_string(global_state.reader != nullptr));
    DUCKDB_GRAPHAR_LOG_DEBUG("Reader started " + to_string(global_state.reader->started()));

    if (global_state.one_hop) {
        if (!global_state.reader->started()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("OneHop started");
            std::unique_ptr<Connection> conn = std::make_unique<Connection>(*context.db);
            global_state.reader->start(std::move(conn));
        }
        data = std::move(global_state.reader->read());
        if (data == nullptr) {
            global_state.one_hop = false;
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG(to_string(data->size()) + " found vertexes");
            for (idx_t i = 0; i < data->size(); i++) {
                global_state.hop_ids.emplace(data->GetValue(1, i).GetValue<int64_t>());
            }
            output.Reference(*data);
            return ;
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("OneHop finished")
        global_state.init_iter();
        DUCKDB_GRAPHAR_LOG_DEBUG("Init set iterator");
        if (global_state.hop_i != global_state.hop_ids.end()) {
            global_state.reader->SetVertex(*global_state.hop_i);
        }
    }
    while (global_state.hop_i != global_state.hop_ids.end()) {
        if (!global_state.reader->started()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("VertexHop started")
            std::unique_ptr<Connection> conn = std::make_unique<Connection>(*context.db);
            global_state.reader->start(std::move(conn));
        }
        data = std::move(global_state.reader->read());

        if (data != nullptr) {
            output.Reference(*data);
            return;
        } else {
            ++global_state.hop_i;
            if (global_state.hop_i != global_state.hop_ids.end()) {
                global_state.reader->SetVertex(*global_state.hop_i);
            }
            DUCKDB_GRAPHAR_LOG_DEBUG("VertexHop finished")
        }
    }
    output.SetCapacity(0);
}
//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
TableFunction TwoHop::GetFunction() {
    TableFunction read_edges("two_hop", {LogicalType::VARCHAR}, Execute, Bind);
    read_edges.init_global = TwoHopGlobalTableFunctionState::Init;
    read_edges.named_parameters["vid"] = LogicalType::INTEGER;

    //	read_edges.filter_pushdown = true;

    return read_edges;
}

TableFunction OneMoreHop::GetFunction() {
    TableFunction read_edges("one_more_hop", {LogicalType::VARCHAR}, Execute, OneMoreHop::Bind);
    read_edges.init_global = OneMoreHopGlobalTableFunctionState::Init;
    read_edges.named_parameters["vid"] = LogicalType::INTEGER;

    //	read_edges.filter_pushdown = true;

    return read_edges;
}
TableFunction FastTwoHop::GetFunction() {
    TableFunction func("fast_two_hop", {LogicalType::VARCHAR}, Execute, TwoHop::Bind);
    func.init_global = FastTwoHopGTFS::Init;
    func.named_parameters["vid"] = LogicalType::INTEGER;

    return func;
}

void TwoHop::Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunction()); }

void OneMoreHop::Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunction()); }

void FastTwoHop::Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunction()); }
}  // namespace duckdb
