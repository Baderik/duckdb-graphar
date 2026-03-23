#pragma once

#include <duckdb/main/connection.hpp>

#include <graphar/arrow/chunk_reader.h>

namespace duckdb {

class QueryChunkReader {
public:
    explicit QueryChunkReader(std::shared_ptr<Connection> conn) : conn(std::move(conn)) {}

	template <bool Stream = false, typename... Args>
    void callQuery(const std::string &query, Args&&... args) {
        if constexpr (Stream) {
            result = conn->SendQuery(query);
        } else {
            result = conn->Query(query, args...);
        }
        chunk = result->Fetch();
    }

    graphar::Status next_chunk() {
        if (chunk != nullptr) {
            return graphar::Status::OK();
        }
        return graphar::Status::IndexError("No more chunks to read!!");
    }

    duckdb::unique_ptr<duckdb::DataChunk> GetChunk() {
        auto cur_chunk = std::move(chunk);
        chunk = result->Fetch();
        return cur_chunk;
    }

    static graphar::Result<std::shared_ptr<QueryChunkReader>> Make(std::shared_ptr<Connection> conn, std::string &query_string, graphar::IdType vid) {
        auto reader = std::make_shared<QueryChunkReader>(std::move(conn));
        reader->callQuery(query_string, vid);
        return reader;
    }

private:
    std::shared_ptr<Connection> conn;
    duckdb::unique_ptr<QueryResult> result;
    duckdb::unique_ptr<duckdb::DataChunk> chunk;
};

} // namespace duckdb