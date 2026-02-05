#include "../SyncApplyEngine.h"

#include <sqlite3.h>
#include <string>
#include <iostream>

namespace watermelondb::platform {

void consoleLog(std::string) {}

} // namespace watermelondb::platform

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

bool querySingleText(sqlite3* db, const char* sql, std::string& out) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    bool ok = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* text = sqlite3_column_text(stmt, 0);
        out = text ? reinterpret_cast<const char*>(text) : "";
        ok = true;
    }
    sqlite3_finalize(stmt);
    return ok;
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

void test_insert_and_update() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, count INTEGER)", error);

    std::string payload = R"({
        "count": 2,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "alpha", "count": 1 } },
          { "_table": "tasks", "row": { "id": "t1", "name": "beta", "count": 2 } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should succeed for created/updated");

    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "row should exist");
    expectTrue(name == "beta", "updated row should reflect last write");

    int count = querySingleInt(db, "SELECT count FROM tasks WHERE id='t1'");
    expectTrue(count == 2, "count should be updated");

    sqlite3_close(db);
}

void test_update_inserts_when_missing() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT)", error);

    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "id": "t2", "name": "gamma" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should insert on updated when missing");

    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t2'", name), "row should exist");
    expectTrue(name == "gamma", "row should match updated payload");

    sqlite3_close(db);
}

void test_deletes() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT)", error);
    execSql(db, "INSERT INTO tasks (id, name) VALUES ('t3', 'delta')", error);

    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "_deleted": true, "id": "t3" }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should delete rows");

    int count = querySingleInt(db, "SELECT COUNT(*) FROM tasks");
    expectTrue(count == 0, "deleted row should be removed");

    sqlite3_close(db);
}

void test_invalid_json() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    bool ok = watermelondb::applySyncPayload(db, "{not json}", error);
    expectTrue(!ok, "applySyncPayload should fail on invalid json");
    expectTrue(!error.empty(), "error should be set on invalid json");
    sqlite3_close(db);
}

void test_json_types_as_text() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, meta TEXT, flag INTEGER)", error);

    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": {
              "id": "t4",
              "meta": { "nested": true, "values": [1, 2, 3] },
              "flag": true
            }
          }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should accept objects/arrays");

    std::string meta;
    expectTrue(querySingleText(db, "SELECT meta FROM tasks WHERE id='t4'", meta), "meta should be stored");
    expectTrue(!meta.empty(), "meta should be json string");

    int flag = querySingleInt(db, "SELECT flag FROM tasks WHERE id='t4'");
    expectTrue(flag == 1, "flag should be stored as 1");

    sqlite3_close(db);
}

void test_delete_chunking() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY)", error);

    for (int i = 0; i < 1000; i++) {
        execSql(db, "INSERT INTO tasks (id) VALUES ('x')", error);
    }

    std::string payload = R"({"count": 1000, "items":[)";
    for (int i = 0; i < 1000; i++) {
        if (i) payload += ",";
        payload += "{\"_table\":\"tasks\",\"_deleted\":true,\"id\":\"x\"}";
    }
    payload += "]}";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should delete in chunks");

    int count = querySingleInt(db, "SELECT COUNT(*) FROM tasks");
    expectTrue(count == 0, "all rows should be deleted");

    sqlite3_close(db);
}

void test_rollback_on_error() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT)", error);

    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "name": "missing_id" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(!ok, "applySyncPayload should fail on missing id");

    int count = querySingleInt(db, "SELECT COUNT(*) FROM tasks");
    expectTrue(count == 0, "transaction should rollback on error");

    sqlite3_close(db);
}

void test_payload_requires_envelope_object() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT)", error);

    std::string payload = R"([
        { "_table": "tasks", "row": { "id": "t6", "name": "direct" } }
      ])";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(!ok, "applySyncPayload should require object envelope payload");

    sqlite3_close(db);
}

void test_envelope_payload_upserts_and_deletes() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT)", error);

    std::string payload = R"({
        "count": 3,
        "items": [
          { "_table": "tasks", "row": { "id": "a1", "name": "alpha" } },
          { "_table": "tasks", "row": { "id": "b2", "name": "bravo" } },
          { "_table": "tasks", "_deleted": true, "id": "a1" }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should accept envelope payload");

    int count = querySingleInt(db, "SELECT COUNT(*) FROM tasks");
    expectTrue(count == 1, "only one row should remain after delete");

    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='b2'", name), "remaining row should exist");
    expectTrue(name == "bravo", "remaining row should match payload");

    sqlite3_close(db);
}

void test_updates_last_sequence_id_ulid() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT)", error);
    execSql(db, "CREATE TABLE local_storage (key TEXT PRIMARY KEY, value TEXT)", error);

    std::string payload = R"({
        "count": 3,
        "items": [
          { "_table": "tasks", "row": { "id": "u1", "name": "alpha" }, "_sequence_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV" },
          { "_table": "tasks", "row": { "id": "u2", "name": "beta" }, "_sequence_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW" },
          { "_table": "tasks", "row": { "id": "u3", "name": "gamma" }, "_sequence_id": "01ARZ3NDEKTSV4RRFFQ69G5FAU" }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should update last_sequence_id for ULIDs");

    std::string sequenceId;
    expectTrue(querySingleText(db, "SELECT value FROM local_storage WHERE key='__watermelon_last_sequence_id'", sequenceId),
               "last_sequence_id should be stored");
    expectTrue(sequenceId == "01ARZ3NDEKTSV4RRFFQ69G5FAW", "last_sequence_id should be the highest ULID");

    sqlite3_close(db);
}

} // namespace

int main() {
    test_insert_and_update();
    test_update_inserts_when_missing();
    test_deletes();
    test_invalid_json();
    test_json_types_as_text();
    test_delete_chunking();
    test_rollback_on_error();
    test_payload_requires_envelope_object();
    test_envelope_payload_upserts_and_deletes();
    test_updates_last_sequence_id_ulid();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SyncApplyEngine tests passed\n";
    return 0;
}
