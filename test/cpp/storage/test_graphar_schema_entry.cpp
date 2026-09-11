#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include "duckdb_graphar_extension.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_storage.hpp"

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension_helper.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

using namespace duckdb;

namespace {

// Attaches a GraphAr graph and returns a reference to its main schema. The
// storage extension is linked into the test binary, so we load it explicitly
// (via ExtensionHelper) and then ATTACH a GraphAr database through SQL.
GraphArSchemaEntry &AttachAndGetMainSchema(DuckDB &duck_db, Connection &con, const std::string &graph_path) {
    // Register the storage extension (idempotent). The extension is linked into
    // the test binary via the generated extension loader.
    auto load_result = ExtensionHelper::LoadExtension(duck_db, "duckdb_graphar");
    (void)load_result;

    auto attach_result = con.Query("ATTACH '" + graph_path + "' as test_db (type duckdb_graphar);");
    REQUIRE(!attach_result->HasError());

    // The ATTACH statement runs and auto-commits in its own transaction, leaving
    // the connection in autocommit mode with no active transaction. Catalog
    // lookups (DatabaseManager::GetDatabase / Catalog::GetCatalogTransaction)
    // require an active transaction, so we start one explicitly.
    con.BeginTransaction();

    auto db = DatabaseManager::Get(*con.context->db).GetDatabase(*con.context, "test_db");
    REQUIRE(db);
    auto &graphar_catalog = db->GetCatalog().Cast<GraphArCatalog>();
    return graphar_catalog.GetMainSchema();
}

}  // namespace

// GraphArSchemaEntry is a read-only schema: every mutation entry point must
// throw NotImplementedException. These are verified against a real attached
// GraphAr catalog.

TEST_CASE("Test GraphArSchemaEntry mutation methods throw", "[graphar_schema]") {
    DuckDB db(nullptr);
    Connection con(db);

    auto &schema = AttachAndGetMainSchema(db, con, "data/snap-musae-github/graphar/Git.graph.yaml");
    auto &catalog = schema.ParentCatalog();
    auto transaction = catalog.GetCatalogTransaction(*con.context);

    // CreateTable throws
    auto base_info = make_uniq<CreateTableInfo>();
    base_info->SetName("test_table");
    BoundCreateTableInfo bound_info(schema, std::move(base_info));
    REQUIRE_THROWS_AS(schema.CreateTable(transaction, bound_info), NotImplementedException);

    // CreateIndex throws
    CreateIndexInfo index_info;
    index_info.constraint_type = IndexConstraintType::NONE;
    index_info.table = "test_table";
    index_info.SetIndexName("test_index");
    auto &tables = schema.tables;
    auto person_table = tables.GetEntry(*con.context,
                                        EntryLookupInfo(CatalogType::TABLE_ENTRY, QualifiedName("Person")));
    REQUIRE(person_table);
    auto &person_table_entry = person_table->Cast<TableCatalogEntry>();
    REQUIRE_THROWS_AS(schema.CreateIndex(transaction, index_info, person_table_entry), NotImplementedException);

    // DropEntry throws
    DropInfo drop_info;
    drop_info.type = CatalogType::SCHEMA_ENTRY;
    drop_info.SetName("test_schema");
    REQUIRE_THROWS_AS(schema.DropEntry(*con.context, drop_info), NotImplementedException);

    con.Commit();
}

// The schema exposes the graph's tables (e.g. the Person vertex table).
TEST_CASE("Test GraphArSchemaEntry exposes graph tables", "[graphar_schema]") {
    DuckDB db(nullptr);
    Connection con(db);

    auto &schema = AttachAndGetMainSchema(db, con, "data/snap-musae-github/graphar/Git.graph.yaml");

    bool found = false;
    schema.Scan(*con.context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
        if (entry.name == "Person") {
            found = true;
        }
    });
    REQUIRE(found);

    con.Commit();
}
