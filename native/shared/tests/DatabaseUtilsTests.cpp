#include "../DatabaseUtils.h"

#include <hermes/hermes.h>
#include <jsi/jsi.h>
#include <sqlite3.h>

#include <iostream>
#include <string>
#include <vector>

namespace {

static int gFailures = 0;

void expectTrue(bool value, const char* message) {
    if (!value) {
        std::cerr << "FAIL: " << message << "\n";
        gFailures++;
    }
}

bool execSql(sqlite3* db, const char* sql, std::string& error) {
    char* errMsg = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        if (errMsg) {
            error = errMsg;
            sqlite3_free(errMsg);
        } else {
            error = "sqlite3_exec failed";
        }
        return false;
    }
    return true;
}

void test_getStmt_and_resultDictionary() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    auto& rt = *runtime;
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;

    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, count INTEGER, active INTEGER)", error);
    execSql(db, "INSERT INTO tasks (id, name, count, active) VALUES ('t1', 'alpha', 5, 1)", error);

    jsi::Array args(rt, 1);
    args.setValueAtIndex(rt, 0, jsi::String::createFromUtf8(rt, "t1"));

    sqlite3_stmt* stmt = watermelondb::getStmt(rt, db, "SELECT name, count, active FROM tasks WHERE id = ?", args);
    int stepResult = sqlite3_step(stmt);
    expectTrue(stepResult == SQLITE_ROW, "expected sqlite row");

    jsi::Object row = watermelondb::resultDictionary(rt, stmt);
    expectTrue(row.getProperty(rt, "name").asString(rt).utf8(rt) == "alpha", "name should match");
    expectTrue(static_cast<int>(row.getProperty(rt, "count").asNumber()) == 5, "count should match");
    expectTrue(row.getProperty(rt, "active").asNumber() == 1, "active should match");

    watermelondb::finalizeStmt(stmt);
    sqlite3_close(db);
}

void test_getStmt_mismatched_args() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    auto& rt = *runtime;
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;

    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY)", error);

    jsi::Array args(rt, 0);
    bool threw = false;
    try {
        watermelondb::getStmt(rt, db, "SELECT id FROM tasks WHERE id = ?", args);
    } catch (const jsi::JSError&) {
        threw = true;
    }
    expectTrue(threw, "expected mismatched args to throw");

    sqlite3_close(db);
}

void test_getStmt_invalid_argument_type() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    auto& rt = *runtime;
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;

    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY)", error);

    jsi::Array args(rt, 1);
    args.setValueAtIndex(rt, 0, jsi::Object(rt));
    bool threw = false;
    try {
        watermelondb::getStmt(rt, db, "SELECT id FROM tasks WHERE id = ?", args);
    } catch (const jsi::JSError&) {
        threw = true;
    }
    expectTrue(threw, "expected object arg to throw");

    sqlite3_close(db);
}

void test_arrayFromStd_and_getNextRowOrTrue() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    auto& rt = *runtime;
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;

    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY)", error);
    execSql(db, "INSERT INTO tasks (id) VALUES ('t1')", error);

    jsi::Array args(rt, 0);
    sqlite3_stmt* stmt = watermelondb::getStmt(rt, db, "SELECT id FROM tasks", args);
    bool done = watermelondb::getNextRowOrTrue(rt, stmt);
    expectTrue(done == false, "expected row available");

    std::vector<jsi::Value> values;
    values.push_back(jsi::String::createFromUtf8(rt, "x"));
    values.push_back(jsi::Value(2));
    jsi::Array arr = watermelondb::arrayFromStd(rt, values);
    expectTrue(arr.length(rt) == 2, "array length should match");

    watermelondb::finalizeStmt(stmt);
    sqlite3_close(db);
}

} // namespace

int main() {
    test_getStmt_and_resultDictionary();
    test_getStmt_mismatched_args();
    test_getStmt_invalid_argument_type();
    test_arrayFromStd_and_getNextRowOrTrue();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All DatabaseUtils tests passed\n";
    return 0;
}
