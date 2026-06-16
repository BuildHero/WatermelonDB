#!/usr/bin/env bash
# MOBILE-5606 — build + run the writer-serialization tests on the macOS host.
# Database.swift is pure Foundation + SQLite3 + FMDB (no React/JSI/Nitro), so
# the real class compiles and runs off-device. No simulator, no Pods.
set -euo pipefail

cd "$(dirname "$0")/.."  # → native/ios

FMDB="WatermelonDB/FMDB/src/fmdb"
TESTS="WatermelonDBTests"
BUILD="/tmp/wmdb-5606-tests"
SDK="$(xcrun --sdk macosx --show-sdk-path)"

rm -rf "$BUILD"; mkdir -p "$BUILD"

echo "== compiling ObjC deps (FMDB + reset shim) =="
OBJC_SRCS=(
  "$FMDB/FMDatabase.m"
  "$FMDB/FMResultSet.m"
  "$FMDB/FMDatabaseAdditions.m"
  "$FMDB/FMDatabaseQueue.m"
  "$FMDB/FMDatabasePool.m"
  "WatermelonDB/DatabaseDeleteHelper.m"
)
for src in "${OBJC_SRCS[@]}"; do
  obj="$BUILD/$(basename "${src%.m}").o"
  clang -c -fobjc-arc -w -isysroot "$SDK" -I "$FMDB" -I WatermelonDB "$src" -o "$obj"
done

echo "== compiling Swift (real Database) + linking =="
swiftc -o "$BUILD/wmdb5606tests" \
  -sdk "$SDK" \
  -import-objc-header "$TESTS/BridgingHeader.h" \
  -Xcc -I"$FMDB" -Xcc -IWatermelonDB \
  WatermelonDB/std_ext.swift \
  WatermelonDB/Database.swift \
  "$TESTS/main.swift" \
  "$BUILD"/*.o \
  -framework Foundation -lsqlite3

echo "== running =="
"$BUILD/wmdb5606tests"
