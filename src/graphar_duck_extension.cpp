#define DUCKDB_EXTENSION_MAIN

#include "graphar_duck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "read_v.hpp"
#include "read_e.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	// Register a table function
	ReadVertices::Register(instance);
	ReadEdges::Register(instance);
}

void GrapharDuckExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string GrapharDuckExtension::Name() {
	return "graphar_duck";
}

std::string GrapharDuckExtension::Version() const {
#ifdef EXT_VERSION_GRAPHAR_DUCK
	return EXT_VERSION_GRAPHAR_DUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void graphar_duck_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::GrapharDuckExtension>();
}

DUCKDB_EXTENSION_API const char *graphar_duck_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
