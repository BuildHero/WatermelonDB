#!/usr/bin/env bash
set -euo pipefail

if ! command -v cmake >/dev/null 2>&1; then
  echo "[cpp tests] cmake not found, skipping"
  exit 0
fi

ROOT_DIR=$(pwd)
SIMDJSON_DIR=${SIMDJSON_INCLUDE_DIR:-$ROOT_DIR/node_modules/@nozbe/simdjson/src}
if [ ! -f "$SIMDJSON_DIR/simdjson.h" ]; then
  echo "[cpp tests] simdjson.h not found at $SIMDJSON_DIR, skipping"
  exit 0
fi

HERMES_ROOT_ENGINE=$ROOT_DIR/node_modules/react-native/sdks/hermes-engine/destroot
HERMES_ROOT_SOURCE=$ROOT_DIR/node_modules/react-native/sdks/hermes/destroot
HERMES_SRC_ROOT=$ROOT_DIR/node_modules/react-native/sdks/hermes
if [ -d "$HERMES_ROOT_ENGINE" ]; then
  HERMES_ROOT=$HERMES_ROOT_ENGINE
else
  HERMES_ROOT=$HERMES_ROOT_SOURCE
fi

HERMES_HEADER=$HERMES_ROOT/include/hermes/hermes.h
HERMES_LIB=$HERMES_ROOT/Library/Frameworks/macosx/hermes.framework/hermes
if [ ! -f "$HERMES_HEADER" ]; then
  HERMES_HEADER=$HERMES_SRC_ROOT/API/hermes/hermes.h
fi
if [ ! -f "$HERMES_LIB" ]; then
  HERMES_LIB=$HERMES_SRC_ROOT/build_macosx/API/hermes/hermes.framework/hermes
fi
if [ -f "$HERMES_ROOT/include/hermes/hermes.h" ]; then
  HERMES_INCLUDE_DIR=$HERMES_ROOT/include
elif [ -f "$HERMES_SRC_ROOT/API/hermes/hermes.h" ]; then
  HERMES_INCLUDE_DIR=$HERMES_SRC_ROOT/API
else
  HERMES_INCLUDE_DIR=""
fi

if [ ! -f "$HERMES_HEADER" ] || [ ! -f "$HERMES_LIB" ]; then
  echo "[cpp tests] Hermes not found, preparing build..."
  CI=true node $ROOT_DIR/node_modules/react-native/scripts/hermes/prepare-hermes-for-build.js
  if [ -f "$ROOT_DIR/node_modules/react-native/sdks/hermes/utils/build-mac-framework.sh" ]; then
    (cd $ROOT_DIR/node_modules/react-native/sdks/hermes && MAC_DEPLOYMENT_TARGET=11.0 REACT_NATIVE_PATH=$ROOT_DIR/node_modules/react-native bash utils/build-mac-framework.sh)
  fi
fi

HERMES_HEADER=$HERMES_ROOT/include/hermes/hermes.h
HERMES_LIB=$HERMES_ROOT/Library/Frameworks/macosx/hermes.framework/hermes
if [ ! -f "$HERMES_HEADER" ]; then
  HERMES_HEADER=$HERMES_SRC_ROOT/API/hermes/hermes.h
fi
if [ ! -f "$HERMES_LIB" ]; then
  HERMES_LIB=$HERMES_SRC_ROOT/build_macosx/API/hermes/hermes.framework/hermes
fi
if [ -f "$HERMES_ROOT/include/hermes/hermes.h" ]; then
  HERMES_INCLUDE_DIR=$HERMES_ROOT/include
elif [ -f "$HERMES_SRC_ROOT/API/hermes/hermes.h" ]; then
  HERMES_INCLUDE_DIR=$HERMES_SRC_ROOT/API
fi

if [ ! -f "$HERMES_HEADER" ] || [ ! -f "$HERMES_LIB" ]; then
  echo "[cpp tests] Hermes still missing after prepare; database_utils_tests will be skipped"
fi

cmake -S native/shared/tests -B native/shared/tests/build \
  -DSIMDJSON_INCLUDE_DIR=$SIMDJSON_DIR \
  -DHERMES_INCLUDE_DIR=$HERMES_INCLUDE_DIR \
  -DHERMES_LIB=$HERMES_LIB
cmake --build native/shared/tests/build

run_test() {
  local name=$1
  local path=$2
  if [ ! -f "$path" ]; then
    echo "[cpp tests] ${name} missing (not built)"
    return 0
  fi
  echo "[cpp tests] running ${name}"
  if "$path"; then
    return 0
  fi
  local status=$?
  echo "[cpp tests] ${name} failed with ${status}"
  return $status
}

run_test "sync_engine_tests" native/shared/tests/build/sync_engine_tests
run_test "sync_apply_engine_tests" native/shared/tests/build/sync_apply_engine_tests
run_test "slice_decoder_tests" native/shared/tests/build/slice_decoder_tests
run_test "slice_import_engine_tests" native/shared/tests/build/slice_import_engine_tests
run_test "sqlite_insert_helper_tests" native/shared/tests/build/sqlite_insert_helper_tests
if [ -f native/shared/tests/build/database_utils_tests ]; then
  run_test "database_utils_tests" native/shared/tests/build/database_utils_tests
else
  echo "[cpp tests] database_utils_tests skipped (Hermes/JSI not found)"
fi

