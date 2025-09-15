#pragma once

#include "utils/func.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension_util.hpp>


#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

namespace duckdb {


class TestBindData final : public TableFunctionData {
public:
    TestBindData(std::string file_1_, std::string file_2_)
        : file_1(file_1_), file_2(file_2_) {};

public:
    std::string file_1;
    std::string file_2;
};

//struct TestGlobalState {
//public:
//    TestGlobalState(ClientContext& context, const TestBindData& bind_data)
//        : file_1(bind_data.file_1), file_2(bind_data.file_2) {};
//
//public:
//    std::string file_1;
//    std::string file_2;
//    unique_ptr<QueryResult> result = nullptr;
//};

struct TestGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    TestGlobalTableFunctionState(ClientContext& context, const TestBindData& bind_data) {};
//        : state(context, bind_data) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

//    TestGlobalState& GetState() { return state; }

//private:
//    TestGlobalState state;
};

struct TestLocalTableFunctionState : public LocalTableFunctionState {
public:
    TestLocalTableFunctionState() {};

    static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context,
                                                    TableFunctionInitInput &input,
                                                    GlobalTableFunctionState *global_state);
    idx_t MaxThreads() const {
        return 12;
    }
public:
    bool first = true;
};

struct TestF {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static TableFunction GetFunction();
    static void Register(DatabaseInstance& db);
};


class TwoHopParquetBindData final : public TableFunctionData {
public:
    TwoHopParquetBindData(std::string edge_info_path, graphar::IdType vid)
        : edge_info_path(edge_info_path), vid(vid) {};

    idx_t MaxThreads() const {
        return 12;
    }
public:
    const std::string edge_info_path;
    const graphar::IdType vid;
};

struct TwoHopParquetGlobalState {
public:
    TwoHopParquetGlobalState(ClientContext& context, const TwoHopParquetBindData& bind_data) {};

    idx_t MaxThreads() const {
        return 12;
    }
public:
    std::queue<std::unique_ptr<LowEdgeReaderByVertex>> vertex_readers;
    std::mutex vertex_readers_mutex;
};

struct TwoHopParquetGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    TwoHopParquetGlobalTableFunctionState(ClientContext& context, const TwoHopParquetBindData& bind_data)
        : state(context, bind_data) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

    TwoHopParquetGlobalState& GetState() { return state; }

    idx_t MaxThreads() const {
        return 12;
    }

private:
    TwoHopParquetGlobalState state;
};

struct TwoHopParquetLocalTableFunctionState : public LocalTableFunctionState {
public:
    TwoHopParquetLocalTableFunctionState() {};

    static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context,
                                                    TableFunctionInitInput &input,
                                                    GlobalTableFunctionState *global_state);
    idx_t MaxThreads() const {
        return 12;
    }
};


struct TwoHopParquet {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static TableFunction GetFunction();
    static void Register(DatabaseInstance& db);
};

}