#include "../SqliteInsertHelper.h"

#include <sqlite3.h>
#include <string>
#include <vector>
#include <iostream>

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

int querySingleInt(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return -1;
    }
    int value = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        value = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return value;
}

std::string querySingleText(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return "";
    }
    std::string value;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* text = sqlite3_column_text(stmt, 0);
        value = text ? reinterpret_cast<const char*>(text) : "";
    }
    sqlite3_finalize(stmt);
    return value;
}

void test_insert_rows_multi_basic() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, count INTEGER, score REAL, data BLOB, _status TEXT)", error);

    watermelondb::SqliteInsertHelper helper;
    std::vector<std::string> columns = {"id", "name", "count", "score", "data"};
    std::vector<std::vector<watermelondb::FieldValue>> rows;
    rows.push_back({
        watermelondb::FieldValue::makeText("t1"),
        watermelondb::FieldValue::makeText("alpha"),
        watermelondb::FieldValue::makeInt(5),
        watermelondb::FieldValue::makeReal(3.5),
        watermelondb::FieldValue::makeBlob({1, 2, 3})
    });
    rows.push_back({
        watermelondb::FieldValue::makeText("t2"),
        watermelondb::FieldValue::makeText("beta"),
        watermelondb::FieldValue::makeNull(),
        watermelondb::FieldValue::makeReal(0.0),
        watermelondb::FieldValue::makeBlob({})
    });

    bool ok = helper.insertRowsMulti(db, "tasks", columns, rows, error);
    expectTrue(ok, "insertRowsMulti should succeed");

    int count = querySingleInt(db, "SELECT COUNT(*) FROM tasks");
    expectTrue(count == 2, "rows should be inserted");

    std::string status = querySingleText(db, "SELECT _status FROM tasks WHERE id='t1'");
    expectTrue(status == "synced", "_status should be synced");

    helper.finalizeStatements();
    sqlite3_close(db);
}

void test_insert_rows_multi_chunking() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE items (id TEXT PRIMARY KEY, _status TEXT)", error);

    watermelondb::SqliteInsertHelper helper;
    std::vector<std::string> columns = {"id"};
    std::vector<std::vector<watermelondb::FieldValue>> rows;
    rows.reserve(1000);
    for (int i = 0; i < 1000; i++) {
        rows.push_back({watermelondb::FieldValue::makeText("x" + std::to_string(i))});
    }

    bool ok = helper.insertRowsMulti(db, "items", columns, rows, error);
    expectTrue(ok, "insertRowsMulti should handle chunking");

    int count = querySingleInt(db, "SELECT COUNT(*) FROM items");
    expectTrue(count == 1000, "chunked insert should insert all rows");

    helper.finalizeStatements();
    sqlite3_close(db);
}

void test_insert_batch_multiple_tables() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE t1 (id TEXT PRIMARY KEY, name TEXT, _status TEXT)", error);
    execSql(db, "CREATE TABLE t2 (id TEXT PRIMARY KEY, title TEXT, _status TEXT)", error);

    watermelondb::BatchData batch;
    batch.addRow("t1", {"id", "name"}, {
        watermelondb::FieldValue::makeText("a"),
        watermelondb::FieldValue::makeText("alpha")
    });
    batch.addRow("t2", {"id", "title"}, {
        watermelondb::FieldValue::makeText("b"),
        watermelondb::FieldValue::makeText("bravo")
    });

    watermelondb::SqliteInsertHelper helper;
    bool ok = helper.insertBatch(db, batch, error);
    expectTrue(ok, "insertBatch should succeed");

    expectTrue(querySingleInt(db, "SELECT COUNT(*) FROM t1") == 1, "t1 row inserted");
    expectTrue(querySingleInt(db, "SELECT COUNT(*) FROM t2") == 1, "t2 row inserted");

    helper.finalizeStatements();
    sqlite3_close(db);
}

} // namespace

int main() {
    test_insert_rows_multi_basic();
    test_insert_rows_multi_chunking();
    test_insert_batch_multiple_tables();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SqliteInsertHelper tests passed\n";
    return 0;
}
