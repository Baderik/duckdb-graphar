# C++ unit tests (Catch2)

This directory holds the extension's **C++ unit tests**, written with
[Catch2](https://github.com/catchorg/Catch2) and compiled into a single
`unittest_graphar` binary.

- `table_functions/` — tests for the read_vertices / read_edges / hop /
  shortest_path table functions.
- `storage/` — tests for the graphar schema / storage layer.
- `scalar_functions/` — scalar function tests (fixture only, for now).
- `fixture/` — shared helpers that generate temporary GraphAr graphs
  (CSV / Parquet) used by the tests.

The binary links directly against `duckdb_static` + the generated extension
loader + the extension's static archive.

Run them with:

```bash
make test-unit           # release (default)
make test-unit-debug     # debug build
```

or invoke the binary directly:

```bash
./build/release/extension/duckdb_graphar/test/cpp/unittest_graphar
```

> The SQL-level (end-to-end) tests live under `test/sql/` (see
> `test/README.md`), not here.
