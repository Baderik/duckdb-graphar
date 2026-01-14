#pragma once

#include "readers/offset_reader.hpp"
#include "utils/global_log_manager.hpp"

#include <arrow/api.h>
#include <arrow/scalar.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table/arrow/arrow_type_info.hpp>
#include <duckdb/function/table/arrow/enum/arrow_type_info_type.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/reader_util.h>
#include <graphar/types.h>

#include <iostream>
#include <math.h>

namespace duckdb {

const std::string GID_COLUMN = "grapharId";
const std::string GID_COLUMN_INTERNAL = "_graphArVertexIndex";
const std::string SRC_GID_COLUMN = "_graphArSrcIndex";
const std::string DST_GID_COLUMN = "_graphArDstIndex";

struct GraphArFunctions {
    static LogicalTypeId graphArT2duckT(const std::string& name);

    static std::shared_ptr<arrow::DataType> graphArT2arrowT(const std::string& name);

    static unique_ptr<ArrowTypeInfo> graphArT2ArrowTypeInfo(const std::string& name);

    static Value ArrowScalar2DuckValue(const std::shared_ptr<arrow::Scalar>& scalar);

    template <typename Info>
    static std::string GetNameFromInfo(const std::shared_ptr<Info>& info);

    static int64_t GetVertexNum(std::shared_ptr<graphar::GraphInfo> graph_info, const std::string& type);

    template <typename GraphArIter>
    static void setByIter(DataChunk& output, GraphArIter& iter, const int prop_i, const int row_i,
                          const std::string& prop_name, const std::string& prop_type) {
        if (prop_type == "bool") {
            output.SetValue(prop_i, row_i, iter.template property<bool>(prop_name).value());
        } else if (prop_type == "int32") {
            output.SetValue(prop_i, row_i, iter.template property<int32_t>(prop_name).value());
        } else if (prop_type == "int64") {
            output.SetValue(prop_i, row_i, iter.template property<int64_t>(prop_name).value());
        } else if (prop_type == "float") {
            output.SetValue(prop_i, row_i, iter.template property<float_t>(prop_name).value());
        } else if (prop_type == "double") {
            output.SetValue(prop_i, row_i, iter.template property<double_t>(prop_name).value());
        } else if (prop_type == "string") {
            output.SetValue(prop_i, row_i, iter.template property<std::string>(prop_name).value());
        } else {
            throw NotImplementedException("Unsupported type");
        }
    };

    static graphar::Result<std::shared_ptr<arrow::Schema>> NamesAndTypesToArrowSchema(const vector<std::string>& names,
                                                                                      const vector<std::string>& types);

    static std::shared_ptr<arrow::Table> EmptyTableFromNamesAndTypes(const vector<std::string>& names,
                                                                     const vector<std::string>& types);

    static std::shared_ptr<graphar::Expression> GetFilter(const std::string& filter_type,
                                                          const std::string& filter_value,
                                                          const std::string& filter_column);
};

static void release_children_only(struct ArrowArray* array) {
    if (array == nullptr) return;

    if (array->children != nullptr) {
        free(array->children);
        array->children = nullptr;
    }

    array->release = nullptr;
}

template <typename Array>
static int64_t GetInt64Value(std::shared_ptr<Array> array, int64_t index) {
    return std::static_pointer_cast<arrow::Int64Scalar>(array->GetScalar(index).ValueOrDie())->value;
}

class LowEdgeReaderByVertex {
public:
    LowEdgeReaderByVertex(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix,
                          graphar::AdjListType adj_list_type, std::shared_ptr<OffsetReader> offset_reader)
        : edge_info(edge_info), prefix(prefix), adj_list_type(adj_list_type), offset_reader(offset_reader) {}

    void SetVertex(graphar::IdType vid) {
        DUCKDB_GRAPHAR_LOG_TRACE("Reader::SetVertex");
        offset = offset_reader->GetOffset(vid);
        vertex_chunk_index = vid / offset_reader->vertex_chunk_size;
        result = nullptr;
    }

    unique_ptr<DataChunk> read() {
        DUCKDB_GRAPHAR_LOG_TRACE("Reader::read");
        if (!started()) {
            throw NotImplementedException("Reader for Vertex not started");
        }
        return std::move(result->Fetch());
    }

    bool started() { return (result != nullptr); }

    void start(std::unique_ptr<Connection> conn) {
        DUCKDB_GRAPHAR_LOG_TRACE("Reader::start");
        auto paths = GetChunkPaths();
        vector<Value> paths_val;
        paths_val.reserve(paths.size());
        for (auto& el : paths) {
            paths_val.emplace_back(prefix + el);
        }
        const std::string query =
            "SELECT #1, #2 FROM read_parquet($1, file_row_number=true) "
            "WHERE file_row_number BETWEEN $2 AND ($2 + $3 - 1);";
        auto offset_in_chunk = offset.first % edge_info->GetChunkSize();
        auto count = offset.second - offset.first;
        Value path_list_val = Value::LIST(paths_val);
        result = std::move(conn->Query(query, path_list_val, offset_in_chunk, count));
    }

    std::vector<std::string> GetChunkPaths() {
        auto range = GetChunkRange();
        auto begin_chunk = range.first, end_chunk = range.second;
        std::vector<std::string> chunks(end_chunk - begin_chunk);
        for (int chunk_index = begin_chunk; chunk_index < end_chunk; ++chunk_index) {
            chunks[chunk_index - begin_chunk] =
                edge_info->GetAdjListFilePath(vertex_chunk_index, chunk_index, adj_list_type).value();
        }

        return std::move(chunks);
    }

    std::pair<graphar::IdType, graphar::IdType> GetChunkRange() {
        auto end_chunk = offset.second / edge_info->GetChunkSize();
        if (offset.second % edge_info->GetChunkSize() != 0) {
            ++end_chunk;
        }

        return std::make_pair(offset.first / edge_info->GetChunkSize(), end_chunk);
    };

    long long size() { return offset.second - offset.first; }

private:
    const std::shared_ptr<graphar::EdgeInfo> edge_info;
    const std::string prefix;
    graphar::AdjListType adj_list_type;
    pair<graphar::IdType, graphar::IdType> offset;
    graphar::IdType vertex_chunk_index;
    std::unique_ptr<QueryResult> result = nullptr;
    std::shared_ptr<OffsetReader> offset_reader;
};

inline void PrintArrowTable(const std::shared_ptr<arrow::Table>& table, int64_t limit = 0) {
    int64_t num_rows = table->num_rows();
    int num_columns = table->num_columns();

    for (int col = 0; col < num_columns; ++col) {
        std::cout << table->field(col)->name();
        if (col < num_columns - 1) std::cout << "\t";
    }
    std::cout << std::endl;
    if (limit > 0) {
        num_rows = std::min(num_rows, limit);
    }

    for (int64_t row = 0; row < num_rows; ++row) {
        for (int col = 0; col < num_columns; ++col) {
            const std::shared_ptr<arrow::ChunkedArray>& chunked_array = table->column(col);
            int64_t local_row = row;
            for (const auto& chunk : chunked_array->chunks()) {
                if (local_row < chunk->length()) {
                    auto scalar_result = chunk->GetScalar(local_row);
                    if (scalar_result.ok()) {
                        std::shared_ptr<arrow::Scalar> scalar = scalar_result.ValueOrDie();
                        std::cout << scalar->ToString();
                    } else {
                        std::cout << "[error]";
                    }
                    break;
                } else {
                    local_row -= chunk->length();
                }
            }
            if (col < num_columns - 1) std::cout << "\t";
        }
        std::cout << std::endl;
    }
}

std::string GetYamlContent(const std::string& path);
std::string GetDirectory(const std::string& path);
std::int64_t GetCount(const std::string& path);
std::int64_t GetVertexCount(const std::shared_ptr<graphar::EdgeInfo>& edge_info, const std::string& directory);

void ConvertArrowTableToDataChunk(const arrow::Table& table, DataChunk& output, const std::vector<column_t>& column_ids,
                                  ClientContext& context);
}  // namespace duckdb
