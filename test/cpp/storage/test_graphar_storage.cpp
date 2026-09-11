#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include "duckdb_graphar_extension.hpp"
#include "storage/graphar_storage.hpp"

#include <duckdb.hpp>

using namespace duckdb;

// The storage extension registers the attach / transaction-manager callbacks
// used by DuckDB's ATTACH machinery. GraphArAttach and
// GraphArCreateTransactionManager are static (file-local) in graphar_storage.cpp
// and are wired into the extension in its constructor, so the test verifies the
// resulting public surface: both callbacks must be non-null after construction.
TEST_CASE("GraphArStorageExtension attaches attach and transaction manager callbacks", "[graphar_storage]") {
    GraphArStorageExtension extension;

    REQUIRE(extension.attach != nullptr);
    REQUIRE(extension.create_transaction_manager != nullptr);
}