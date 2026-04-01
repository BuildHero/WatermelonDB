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

// --- Conflict resolution tests: _status preservation during sync pull ---

void test_created_record_not_overwritten() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, type TEXT, _status TEXT, _changed TEXT)", error);

    // Insert a locally-created record (not yet pushed to server)
    execSql(db, "INSERT INTO tasks (id, name, type, _status, _changed) VALUES ('t1', 'local-name', 'before', 'created', '')", error);

    // Server sends a version of this record with different values
    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "server-name", "type": null } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should succeed for created record");

    // Local record should be preserved — NOT overwritten by server data
    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "row should still exist");
    expectTrue(name == "local-name", "created record name should be preserved");

    std::string type;
    expectTrue(querySingleText(db, "SELECT type FROM tasks WHERE id='t1'", type), "type should exist");
    expectTrue(type == "before", "created record type should be preserved");

    std::string status;
    expectTrue(querySingleText(db, "SELECT _status FROM tasks WHERE id='t1'", status), "_status should exist");
    expectTrue(status == "created", "_status should remain 'created'");

    sqlite3_close(db);
}

void test_updated_record_partial_merge() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, description TEXT, priority INTEGER, _status TEXT, _changed TEXT)", error);

    // Insert a locally-updated record: user changed 'name' and 'priority'
    execSql(db, "INSERT INTO tasks (id, name, description, priority, _status, _changed) "
                "VALUES ('t1', 'local-name', 'old-desc', 99, 'updated', 'name,priority')", error);

    // Server sends updated values for all columns
    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "server-name", "description": "new-desc", "priority": 1 } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should succeed for updated record merge");

    // Locally-changed columns should be preserved
    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "name should exist");
    expectTrue(name == "local-name", "locally-changed 'name' should be preserved");

    int priority = querySingleInt(db, "SELECT priority FROM tasks WHERE id='t1'");
    expectTrue(priority == 99, "locally-changed 'priority' should be preserved");

    // Non-changed columns should take server values
    std::string description;
    expectTrue(querySingleText(db, "SELECT description FROM tasks WHERE id='t1'", description), "description should exist");
    expectTrue(description == "new-desc", "non-changed 'description' should take server value");

    // _status and _changed should be preserved
    std::string status;
    expectTrue(querySingleText(db, "SELECT _status FROM tasks WHERE id='t1'", status), "_status should exist");
    expectTrue(status == "updated", "_status should remain 'updated'");

    std::string changed;
    expectTrue(querySingleText(db, "SELECT _changed FROM tasks WHERE id='t1'", changed), "_changed should exist");
    expectTrue(changed == "name,priority", "_changed should be preserved");

    sqlite3_close(db);
}

void test_synced_record_full_overwrite() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, _status TEXT, _changed TEXT)", error);

    // Insert a synced record (no local changes)
    execSql(db, "INSERT INTO tasks (id, name, _status, _changed) VALUES ('t1', 'old-name', NULL, '')", error);

    // Server sends updated values
    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "new-name" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should succeed for synced record");

    // Synced record should be fully overwritten
    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "row should exist");
    expectTrue(name == "new-name", "synced record should be fully overwritten");

    sqlite3_close(db);
}

void test_deleted_record_not_overwritten() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, _status TEXT, _changed TEXT)", error);

    // Insert a locally-deleted record (deletion pending push)
    execSql(db, "INSERT INTO tasks (id, name, _status, _changed) VALUES ('t1', 'local-name', 'deleted', '')", error);

    // Server sends an update for this record
    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "server-name" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should succeed for deleted record");

    // Locally-deleted record should be preserved — NOT overwritten
    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "row should still exist");
    expectTrue(name == "local-name", "deleted record should not be overwritten");

    std::string status;
    expectTrue(querySingleText(db, "SELECT _status FROM tasks WHERE id='t1'", status), "_status should exist");
    expectTrue(status == "deleted", "_status should remain 'deleted'");

    sqlite3_close(db);
}

void test_updated_record_all_changed_skips_update() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, description TEXT, _status TEXT, _changed TEXT)", error);

    // All columns are in _changed — nothing for server to update
    execSql(db, "INSERT INTO tasks (id, name, description, _status, _changed) "
                "VALUES ('t1', 'local-name', 'local-desc', 'updated', 'name,description')", error);

    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "server-name", "description": "server-desc" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should succeed when all columns are locally changed");

    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "name should exist");
    expectTrue(name == "local-name", "all-changed record should preserve all local values");

    std::string description;
    expectTrue(querySingleText(db, "SELECT description FROM tasks WHERE id='t1'", description), "description should exist");
    expectTrue(description == "local-desc", "all-changed record should preserve all local values");

    sqlite3_close(db);
}

void test_mixed_dirty_and_synced_records() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    execSql(db, "CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, _status TEXT, _changed TEXT)", error);

    // Mix of dirty and synced records
    execSql(db, "INSERT INTO tasks (id, name, _status, _changed) VALUES ('t1', 'created-local', 'created', '')", error);
    execSql(db, "INSERT INTO tasks (id, name, _status, _changed) VALUES ('t2', 'synced-old', NULL, '')", error);
    execSql(db, "INSERT INTO tasks (id, name, _status, _changed) VALUES ('t3', 'updated-local', 'updated', 'name')", error);

    std::string payload = R"({
        "count": 4,
        "items": [
          { "_table": "tasks", "row": { "id": "t1", "name": "server-t1" } },
          { "_table": "tasks", "row": { "id": "t2", "name": "server-t2" } },
          { "_table": "tasks", "row": { "id": "t3", "name": "server-t3" } },
          { "_table": "tasks", "row": { "id": "t4", "name": "server-t4" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should handle mixed dirty/synced records");

    // t1: created — should be preserved
    std::string name;
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t1'", name), "t1 should exist");
    expectTrue(name == "created-local", "created record t1 should be preserved");

    // t2: synced — should be overwritten
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t2'", name), "t2 should exist");
    expectTrue(name == "server-t2", "synced record t2 should be overwritten");

    // t3: updated with name in _changed — name should be preserved
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t3'", name), "t3 should exist");
    expectTrue(name == "updated-local", "updated record t3 should preserve changed column");

    // t4: new record — should be inserted
    expectTrue(querySingleText(db, "SELECT name FROM tasks WHERE id='t4'", name), "t4 should be inserted");
    expectTrue(name == "server-t4", "new record t4 should have server values");

    sqlite3_close(db);
}

void test_table_without_status_columns() {
    sqlite3* db = nullptr;
    sqlite3_open(":memory:", &db);
    std::string error;
    // Table without _status/_changed columns (non-synced table)
    execSql(db, "CREATE TABLE settings (id TEXT PRIMARY KEY, value TEXT)", error);

    std::string payload = R"({
        "count": 1,
        "items": [
          { "_table": "settings", "row": { "id": "s1", "value": "hello" } }
        ]
      })";

    bool ok = watermelondb::applySyncPayload(db, payload, error);
    expectTrue(ok, "applySyncPayload should handle tables without _status column");

    std::string value;
    expectTrue(querySingleText(db, "SELECT value FROM settings WHERE id='s1'", value), "row should exist");
    expectTrue(value == "hello", "value should match payload");

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

    // Conflict resolution tests
    test_created_record_not_overwritten();
    test_updated_record_partial_merge();
    test_synced_record_full_overwrite();
    test_deleted_record_not_overwritten();
    test_updated_record_all_changed_skips_update();
    test_mixed_dirty_and_synced_records();
    test_table_without_status_columns();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SyncApplyEngine tests passed\n";
    return 0;
}
