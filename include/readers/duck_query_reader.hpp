#pragma once

#include "readers/base_reader.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <graphar/result.h>

#include <duckdb.hpp>

namespace duckdb {

class DuckQueryChunkReader {
public:
    DuckQueryChunkReader(std::shared_ptr<graphar::TSQueryChunkReader> init_base, ClientContext& init_context)
        : base(std::move(init_base)), context(init_context) {}

    static graphar::Result<std::shared_ptr<DuckQueryChunkReader>> Make(ClientContext& context,
                                                                       std::shared_ptr<graphar::TSQueryChunkReader> base_ptr) {
        if (!base_ptr) {
            return graphar::Status::Invalid("base_ptr can't be null!");
        }
        return std::make_shared<DuckQueryChunkReader>(std::move(base_ptr), context);
    }

    template <typename... Args>
    static graphar::Result<std::shared_ptr<DuckQueryChunkReader>> Make(ClientContext& context, Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, graphar::TSQueryChunkReader::Make(std::forward<Args>(args)...));
        return std::make_shared<DuckQueryChunkReader>(std::move(base_ptr), context);
    }

    idx_t ReserveRowsToRead() {
        if (NoMoreRows()) {
            auto gc_result = base->GetChunk();
            if (gc_result.no_more_chunks || gc_result.chunk == nullptr) {
                return 0;
            }
            cur_chunk = std::move(gc_result.chunk);
            read_rows = 0;
        }

        return cur_chunk->size() - read_rows;
    }

    const inline bool NoMoreRows() { return !cur_chunk || read_rows == cur_chunk->size(); }

    graphar::Result<unique_ptr<DataChunk>> GetChunk(idx_t num_rows) {
        if (ReserveRowsToRead() == 0) {
            throw graphar::Status::IndexError("No more chunks to read!");
        }
        if (num_rows > cur_chunk->size() - read_rows) {
            throw graphar::Status::IndexError("Can't read this many rows");
        }
        auto res = make_uniq<DataChunk>();
        res->Initialize(context, cur_chunk->GetTypes());
        res->Reference(*cur_chunk);
        res->Slice(read_rows, num_rows);
        read_rows += num_rows;
        return res;
    }

    void SelectColumns(std::vector<column_t> proj_columns_) { proj_columns = std::move(proj_columns_); }

private:
    std::vector<column_t> proj_columns;
    ClientContext& context;
    std::shared_ptr<graphar::TSQueryChunkReader> base;
    idx_t read_rows = 0;
    unique_ptr<DataChunk> cur_chunk = nullptr;
};

}  // namespace duckdb

namespace graphar {

using DuckQueryChunkReader = duckdb::DuckQueryChunkReader;

}  // namespace graphar