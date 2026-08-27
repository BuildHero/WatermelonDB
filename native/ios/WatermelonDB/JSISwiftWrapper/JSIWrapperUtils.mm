#include "JSIWrapperUtils.h"
#include "DatabaseUtils.h"
#include <string>
#include <cctype>

namespace watermelondb {

// Temp tables are per-connection and only exist on the writer.
// Route queries referencing temp tables to the writer connection.
static bool referencesTemporaryTable(const std::string &query) {
    // Create a lowercase copy of the query
    std::string lower = query;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Check for temp table references
    return lower.find("temp.") != std::string::npos ||
           lower.find("sqlite_temp_master") != std::string::npos;
}

static bool isReadOnlyQuery(const std::string &query) {
    if (referencesTemporaryTable(query)) {
        return false;
    }
    size_t i = 0;
    while (i < query.size() && std::isspace(static_cast<unsigned char>(query[i]))) {
        i++;
    }
    std::string prefix;
    for (; i < query.size() && prefix.size() < 7; i++) {
        char c = static_cast<char>(std::tolower(static_cast<unsigned char>(query[i])));
        prefix.push_back(c);
    }
    return prefix.rfind("select", 0) == 0 || prefix.rfind("with", 0) == 0 || prefix.rfind("explain", 0) == 0;
}

// A nil module or a nil raw connection has to be reported as what it is. Handing the
// resulting NULL `sqlite3*` to getStmt() is what produced
// "sqlite error 7 (out of memory)" against a perfectly healthy database, because
// sqlite reports a null handle as SQLITE_NOMEM.
static void requireDatabaseBridge(jsi::Runtime &rt, DatabaseBridge *databaseBridge) {
    if (databaseBridge == nil) {
        throw jsi::JSError(rt, "WatermelonDB: DatabaseBridge native module is not available");
    }
}

static sqlite3 *requireConnection(jsi::Runtime &rt, void *rawConnection, const char *which) {
    if (rawConnection == nullptr) {
        throw jsi::JSError(rt, std::string("WatermelonDB: no ") + which +
                           " SQLite connection for this tag - it is absent, or was not set up "
                           "synchronously (the JSI path requires a synchronous adapter)");
    }
    return static_cast<sqlite3 *>(rawConnection);
}

jsi::Value execSqlQuery(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &args) {
   auto tagNumber = [[NSNumber alloc] initWithDouble:tag.asNumber()];

    requireDatabaseBridge(rt, databaseBridge);

    const auto query = sql.utf8(rt);
    const bool readOnly = isReadOnlyQuery(query);
    auto rawDb = readOnly
        ? [databaseBridge getRawReadConnectionWithConnectionTag:tagNumber]
        : [databaseBridge getRawConnectionWithConnectionTag:tagNumber];
    sqlite3 *db = requireConnection(rt, rawDb, readOnly ? "reader" : "writer");

    StmtGuard stmtGuard(getStmt(rt, db, query, args));
    sqlite3_stmt *stmt = stmtGuard.get();

    std::vector<jsi::Value> records = {};

    while (true) {
        if (getNextRowOrTrue(rt, stmt)) {
            break;
        }

        jsi::Object record = resultDictionary(rt, stmt);

        records.push_back(std::move(record));
    }

    // stmtGuard finalizes, including on the throw paths above.
    return arrayFromStd(rt, records);
}

jsi::Value execSqlQueryOnWriter(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &args) {
    auto tagNumber = [[NSNumber alloc] initWithDouble:tag.asNumber()];

    requireDatabaseBridge(rt, databaseBridge);

    const auto query = sql.utf8(rt);
    // Always use the writer connection
    sqlite3 *db = requireConnection(rt, [databaseBridge getRawConnectionWithConnectionTag:tagNumber], "writer");

    StmtGuard stmtGuard(getStmt(rt, db, query, args));
    sqlite3_stmt *stmt = stmtGuard.get();

    std::vector<jsi::Value> records = {};

    while (true) {
        if (getNextRowOrTrue(rt, stmt)) {
            break;
        }

        jsi::Object record = resultDictionary(rt, stmt);

        records.push_back(std::move(record));
    }

    // stmtGuard finalizes, including on the throw paths above.
    return arrayFromStd(rt, records);
}

jsi::Value query(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &table, const jsi::String &query) {
    requireDatabaseBridge(rt, databaseBridge);

    auto tagNumber = [[NSNumber alloc] initWithDouble:tag.asNumber()];
    auto tableStr = [NSString stringWithUTF8String:table.utf8(rt).c_str()];

    sqlite3 *db = requireConnection(rt, [databaseBridge getRawReadConnectionWithConnectionTag:tagNumber], "reader");

    StmtGuard stmtGuard(getStmt(rt, db, query.utf8(rt), jsi::Array(rt, 0)));
    sqlite3_stmt *stmt = stmtGuard.get();

    std::vector<jsi::Value> records = {};

    while (true) {
        if (getNextRowOrTrue(rt, stmt)) {
            break;
        }

        assert(std::string(sqlite3_column_name(stmt, 0)) == "id");

        const char *id = (const char *)sqlite3_column_text(stmt, 0);

        if (!id) {
            throw jsi::JSError(rt, "Failed to get ID of a record");
        }

        auto idStr = [NSString stringWithUTF8String:id];

        bool isCached = [databaseBridge isCachedWithConnectionTag:tagNumber table:tableStr id:idStr];

        if (isCached) {
            jsi::String jsiId = jsi::String::createFromAscii(rt, id);
            records.push_back(std::move(jsiId));
        } else {
            [databaseBridge markAsCachedWithConnectionTag:tagNumber table:tableStr id:idStr];
            jsi::Object record = resultDictionary(rt, stmt);
            records.push_back(std::move(record));
        }
    }

    // stmtGuard finalizes, including on the throw paths above.
    return arrayFromStd(rt, records);
}

} // namespace watermelondb
