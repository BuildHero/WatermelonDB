//
//  DatabaseUtils.cpp
//  WatermelonDB
//
//  Created by BuildOpsLA27 on 10/4/24.
//

#include "DatabaseUtils.h"

namespace watermelondb {

jsi::JSError dbError(jsi::Runtime &rt, sqlite3* db, std::string description) {
    // sqlite maps a NULL connection to SQLITE_NOMEM / "out of memory" in BOTH
    // sqlite3_errmsg and sqlite3_extended_errcode. Reading the code off the handle
    // therefore reports a wiring bug as an allocation failure, which is how a nil
    // DatabaseBridge spent a long time masquerading as "sqlite error 7 (out of
    // memory)" on a perfectly healthy database. Say what actually happened.
    if (db == nullptr) {
        return jsi::JSError(rt, description + " - no SQLite connection (null database handle)");
    }
    // TODO: In serialized threading mode, those may be incorrect - probably smarter to pass result codes around?
    auto sqliteMessage = std::string(sqlite3_errmsg(db));
    auto code = sqlite3_extended_errcode(db);
    auto message = description + " - sqlite error " + std::to_string(code) + " (" + sqliteMessage + ")";
    return jsi::JSError(rt, message);
}

sqlite3_stmt* getStmt(jsi::Runtime &rt, sqlite3* db, std::string sql, const jsi::Array &arguments) {
    if (db == nullptr) {
        throw dbError(rt, db, "Failed to prepare query statement");
    }

    // Must be initialized: sqlite3_prepare_v2 is not guaranteed to write *ppStmt on
    // every early-out, and the failure path below finalizes it.
    sqlite3_stmt *statement = nullptr;

    int resultPrepare = sqlite3_prepare_v2(db, sql.c_str(), -1, &statement, nullptr);

    if (resultPrepare != SQLITE_OK) {
        sqlite3_finalize(statement); // no-op on nullptr
        // Include the actual prepare result. dbError() re-derives its code from the
        // handle, which can disagree with the call's own return value (a NULL handle
        // returns SQLITE_MISUSE here but reports SQLITE_NOMEM there).
        throw dbError(rt, db,
                      "Failed to prepare query statement (prepare rc " + std::to_string(resultPrepare) + ")");
    }

    assert(statement != nullptr);

    // Guard the statement from here on: every throwing path below - including a
    // JSI call like arguments.length()/getValueAtIndex()/getString() itself
    // throwing (e.g. OOM) - now finalizes via the destructor instead of relying
    // on an explicit sqlite3_finalize() at each call site. release() hands back
    // the raw pointer only on the success path at the bottom.
    StmtGuard guard(statement);

    int argsCount = sqlite3_bind_parameter_count(statement);

    if (argsCount != arguments.length(rt)) {
        throw jsi::JSError(rt, "Number of args passed to query doesn't match number of arg placeholders");
    }

    for (int i = 0; i < argsCount; i++) {
        jsi::Value value = arguments.getValueAtIndex(rt, i);

        int bindResult;
        if (value.isNull() || value.isUndefined()) {
            bindResult = sqlite3_bind_null(statement, i + 1);
        } else if (value.isString()) {
            // TODO: Check SQLITE_STATIC
            bindResult = sqlite3_bind_text(statement, i + 1, value.getString(rt).utf8(rt).c_str(), -1, SQLITE_TRANSIENT);
        } else if (value.isNumber()) {
            bindResult = sqlite3_bind_double(statement, i + 1, value.getNumber());
        } else if (value.isBool()) {
            bindResult = sqlite3_bind_int(statement, i + 1, value.getBool());
        } else if (value.isObject()) {
            throw jsi::JSError(rt, "Invalid argument type (object) for query");
        } else {
            throw jsi::JSError(rt, "Invalid argument type (unknown) for query");
        }

        if (bindResult != SQLITE_OK) {
            throw dbError(rt, db, "Failed to bind an argument for query");
        }
    }

    return guard.release();
}

void finalizeStmt(sqlite3_stmt* stmt) {
    sqlite3_finalize(stmt);
}

jsi::Array arrayFromStd(jsi::Runtime &rt, std::vector<jsi::Value> &vector) {
    // FIXME: Adding directly to a jsi::Array should be more efficient, but Hermes does not support
    // automatically resizing an Array by setting new values to it
    jsi::Array array(rt, vector.size());
    
    size_t i = 0;
    
    for (auto const &value : vector) {
        array.setValueAtIndex(rt, i, value);
        i++;
    }
    
    return array;
}

jsi::Object resultDictionary(jsi::Runtime &rt, sqlite3_stmt *statement) {
    jsi::Object dictionary(rt);

    for (int i = 0, len = sqlite3_column_count(statement); i < len; i++) {
        const char *column = sqlite3_column_name(statement, i);
        assert(column);

        auto type = sqlite3_column_type(statement, i);
        if (type == SQLITE_INTEGER) {
            sqlite3_int64 value = sqlite3_column_int64(statement, i);
            dictionary.setProperty(rt, column, jsi::Value((double)value));
        } else if (type == SQLITE_FLOAT) {
            double value = sqlite3_column_double(statement, i);
            dictionary.setProperty(rt, column, jsi::Value(value));
        } else if (type == SQLITE_TEXT) {
            const char *text = (const char *)sqlite3_column_text(statement, i);
            if (text) {
                dictionary.setProperty(rt, column, jsi::String::createFromUtf8(rt, text));
            } else {
                dictionary.setProperty(rt, column, jsi::Value::null());
            }
        } else if (type == SQLITE_NULL) {
            dictionary.setProperty(rt, column, jsi::Value::null());
        } else {
            throw jsi::JSError(rt, "Unable to fetch record from database - unknown column type (WatermelonDB does not support blobs or custom sqlite types");
        }
    }

    return dictionary; // TODO: Make sure this value is moved, not copied
}

bool getNextRowOrTrue(jsi::Runtime &rt, sqlite3_stmt *stmt) {
    int result = sqlite3_step(stmt);

    if (result == SQLITE_DONE) {
        return true;
    } else if (result != SQLITE_ROW) {
        throw jsi::JSError(rt, "Failed to get a row for query");
    }

    return false;
}


}
