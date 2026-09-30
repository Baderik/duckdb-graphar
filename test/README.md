# SQL tests (SQLLogicTests)

SQL end-to-end tests against the graph datasets under `data/`, living in
`test/sql/graphar/`. They are discovered and run by DuckDB's own `unittest`
binary (registered via the `LOAD_TESTS` flag in `extension_config.cmake`,
grouped under the `[graphar]` tag).

```bash
make test-sql            # release (default)
make test-sql-debug      # debug build
./build/release/test/unittest "[graphar]"   # or invoke directly
```

> The C++ unit tests live under `test/cpp/` (see `test/cpp/README.md`).
