#pragma once

#include "JsonUtils.h"

#include <sqlite3.h>
#include <map>
#include <string>
#include <vector>

namespace watermelondb {

// MOBILE-6276: the set of record ids a pull actually wrote (upserted: full-overwrite or
// partial-merge) or hard-deleted, per table. Locally-dirty rows the engine skips are NOT recorded.
struct TableChangeset {
    std::vector<std::string> upserted;
    std::vector<std::string> deleted;
};

// std::map keeps table order deterministic (sorted) for stable JSON + test assertions.
using SyncChangeset = std::map<std::string, TableChangeset>;

// Applies a pulled page to SQLite and APPENDS the ids it committed into `changeset`
// (so a caller can accumulate across paginated pages). Returns false on parse/apply error.
bool applySyncPayload(sqlite3* db, const std::string& payload, std::string& errorMessage,
                      SyncChangeset& changeset);

// Back-compat overload for callers that don't need the changeset.
bool applySyncPayload(sqlite3* db, const std::string& payload, std::string& errorMessage);

// Serializes a changeset to `{"<table>":{"upserted":[...],"deleted":[...]}}`. Empty -> "{}".
// Header-only (no SQLite) so the sync engine can serialize without linking the apply engine.
inline std::string serializeChangeset(const SyncChangeset& changeset) {
    std::string out = "{";
    bool firstTable = true;
    for (const auto& tablePair : changeset) {
        const TableChangeset& tc = tablePair.second;
        if (tc.upserted.empty() && tc.deleted.empty()) {
            continue;
        }
        if (!firstTable) {
            out += ",";
        }
        firstTable = false;
        out += "\"" + json_utils::escapeJsonString(tablePair.first) + "\":{\"upserted\":[";
        for (size_t i = 0; i < tc.upserted.size(); i++) {
            if (i) out += ",";
            out += "\"" + json_utils::escapeJsonString(tc.upserted[i]) + "\"";
        }
        out += "],\"deleted\":[";
        for (size_t i = 0; i < tc.deleted.size(); i++) {
            if (i) out += ",";
            out += "\"" + json_utils::escapeJsonString(tc.deleted[i]) + "\"";
        }
        out += "]}";
    }
    out += "}";
    return out;
}

} // namespace watermelondb
