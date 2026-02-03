# Shared C++ Tests

This directory contains small standalone test executables for the shared C++ sync code.

## Prereqs

- CMake 3.16+
- A C++17 compiler
- SQLite3 development headers
- simdjson headers (provided by `@nozbe/simdjson`)

## Build & run

```sh
cd native/shared/tests
cmake -S . -B build -DSIMDJSON_INCLUDE_DIR=../../../node_modules/@nozbe/simdjson/src
cmake --build build
./build/sync_engine_tests
./build/sync_apply_engine_tests
./build/slice_decoder_tests
./build/slice_import_engine_tests
./build/sqlite_insert_helper_tests
./build/database_utils_tests
```

Notes:
- `SIMDJSON_INCLUDE_DIR` should point at the directory containing `simdjson.h`.
- `database_utils_tests` requires Hermes + JSI headers/libs. It is skipped if not found.
- `yarn test:cpp` will attempt to prepare/build Hermes if missing (requires network + Xcode tools).
