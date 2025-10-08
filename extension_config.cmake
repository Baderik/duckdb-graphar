# Configuration for the DuckDB 'duckdb_graphar' extension.
#

duckdb_extension_load(duckdb_graphar
    SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}"
    LOAD_TESTS
)
