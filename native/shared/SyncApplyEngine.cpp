#include "SyncApplyEngine.h"
#include "JsonUtils.h"

#include <algorithm>
#include <cctype>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if __has_include(<simdjson.h>)
#include <simdjson.h>
#elif __has_include("simdjson.h")
#include "simdjson.h"
#else
#error "simdjson headers not found. Please add @nozbe/simdjson or provide simdjson headers."
#endif

namespace watermelondb {
namespace {

struct JsonValue {
    enum class Type { Null, Bool, Number, String, Array, Object };
    Type type = Type::Null;
    bool boolValue = false;
    std::string stringValue;
    std::string numberValue;
    std::vector<JsonValue> arrayValue;
    std::unordered_map<std::string, JsonValue> objectValue;
};

static bool convertSimdjsonElement(const simdjson::dom::element& element, JsonValue& out, std::string& errorMessage) {
    using simdjson::dom::element_type;
    auto type = element.type();
    switch (type) {
        case element_type::OBJECT: {
            auto objResult = element.get_object();
            if (objResult.error()) {
                errorMessage = simdjson::error_message(objResult.error());
                return false;
            }
            out.type = JsonValue::Type::Object;
            for (auto field : objResult.value()) {
                std::string key(field.key);
                JsonValue value;
                if (!convertSimdjsonElement(field.value, value, errorMessage)) {
                    return false;
                }
                out.objectValue.emplace(std::move(key), std::move(value));
            }
            return true;
        }
        case element_type::ARRAY: {
            auto arrayResult = element.get_array();
            if (arrayResult.error()) {
                errorMessage = simdjson::error_message(arrayResult.error());
                return false;
            }
            out.type = JsonValue::Type::Array;
            for (auto item : arrayResult.value()) {
                JsonValue value;
                if (!convertSimdjsonElement(item, value, errorMessage)) {
                    return false;
                }
                out.arrayValue.emplace_back(std::move(value));
            }
            return true;
        }
        case element_type::STRING: {
            auto stringResult = element.get_string();
            if (stringResult.error()) {
                errorMessage = simdjson::error_message(stringResult.error());
                return false;
            }
            out.type = JsonValue::Type::String;
            out.stringValue.assign(stringResult.value().data(), stringResult.value().size());
            return true;
        }
        case element_type::INT64: {
            auto numberResult = element.get_int64();
            if (numberResult.error()) {
                errorMessage = simdjson::error_message(numberResult.error());
                return false;
            }
            out.type = JsonValue::Type::Number;
            out.numberValue = std::to_string(numberResult.value());
            return true;
        }
        case element_type::UINT64: {
            auto numberResult = element.get_uint64();
            if (numberResult.error()) {
                errorMessage = simdjson::error_message(numberResult.error());
                return false;
            }
            out.type = JsonValue::Type::Number;
            out.numberValue = std::to_string(numberResult.value());
            return true;
        }
        case element_type::DOUBLE: {
            auto numberResult = element.get_double();
            if (numberResult.error()) {
                errorMessage = simdjson::error_message(numberResult.error());
                return false;
            }
            out.type = JsonValue::Type::Number;
            out.numberValue = std::to_string(numberResult.value());
            return true;
        }
        case element_type::BOOL: {
            auto boolResult = element.get_bool();
            if (boolResult.error()) {
                errorMessage = simdjson::error_message(boolResult.error());
                return false;
            }
            out.type = JsonValue::Type::Bool;
            out.boolValue = boolResult.value();
            return true;
        }
        case element_type::NULL_VALUE:
            out.type = JsonValue::Type::Null;
            return true;
    }

    errorMessage = "Unsupported JSON element";
    return false;
}

static bool parseJsonWithSimdjson(const std::string& payload, JsonValue& out, std::string& errorMessage) {
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    simdjson::padded_string json(payload);
    auto error = parser.parse(json).get(doc);
    if (error) {
        errorMessage = simdjson::error_message(error);
        return false;
    }
    return convertSimdjsonElement(doc, out, errorMessage);
}

static std::string toJson(const JsonValue& value) {
    switch (value.type) {
        case JsonValue::Type::Null:
            return "null";
        case JsonValue::Type::Bool:
            return value.boolValue ? "true" : "false";
        case JsonValue::Type::Number:
            return value.numberValue;
        case JsonValue::Type::String:
            return std::string("\"") + json_utils::escapeJsonString(value.stringValue) + "\"";
        case JsonValue::Type::Array: {
            std::string out = "[";
            for (size_t i = 0; i < value.arrayValue.size(); i++) {
                if (i) out += ",";
                out += toJson(value.arrayValue[i]);
            }
            out += "]";
            return out;
        }
        case JsonValue::Type::Object: {
            std::string out = "{";
            bool first = true;
            for (const auto& kv : value.objectValue) {
                if (!first) out += ",";
                first = false;
                out += "\"";
                out += json_utils::escapeJsonString(kv.first);
                out += "\":";
                out += toJson(kv.second);
            }
            out += "}";
            return out;
        }
    }
    return "null";
}

static std::string quoteIdentifier(const std::string& name) {
    std::string out;
    out.reserve(name.size() + 2);
    out.push_back('"');
    for (char c : name) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

static bool bindValue(sqlite3_stmt* stmt, int index, const JsonValue& value) {
    switch (value.type) {
        case JsonValue::Type::Null:
            return sqlite3_bind_null(stmt, index) == SQLITE_OK;
        case JsonValue::Type::Bool:
            return sqlite3_bind_int(stmt, index, value.boolValue ? 1 : 0) == SQLITE_OK;
        case JsonValue::Type::Number: {
            const std::string& num = value.numberValue;
            if (num.find_first_of(".eE") == std::string::npos) {
                try {
                    long long v = std::stoll(num);
                    return sqlite3_bind_int64(stmt, index, static_cast<sqlite3_int64>(v)) == SQLITE_OK;
                } catch (...) {
                }
            }
            double d = std::strtod(num.c_str(), nullptr);
            return sqlite3_bind_double(stmt, index, d) == SQLITE_OK;
        }
        case JsonValue::Type::String:
            return sqlite3_bind_text(stmt, index, value.stringValue.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK;
        case JsonValue::Type::Array:
        case JsonValue::Type::Object: {
            std::string json = toJson(value);
            return sqlite3_bind_text(stmt, index, json.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK;
        }
    }
    return sqlite3_bind_null(stmt, index) == SQLITE_OK;
}

static bool execSql(sqlite3* db, const char* sql, std::string& errorMessage) {
    char* errMsg = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        if (errMsg) {
            errorMessage = errMsg;
            sqlite3_free(errMsg);
        } else {
            errorMessage = "SQLite exec failed";
        }
        return false;
    }
    return true;
}

static bool applyRowObject(sqlite3* db, const std::string& table, const JsonValue& rowValue,
                           std::string& errorMessage);

static bool applyRows(sqlite3* db, const std::string& table, const JsonValue& rows, std::string& errorMessage) {
    if (rows.type != JsonValue::Type::Array) {
        return true;
    }
    for (const auto& rowValue : rows.arrayValue) {
        if (rowValue.type != JsonValue::Type::Object) {
            continue;
        }
        if (!applyRowObject(db, table, rowValue, errorMessage)) {
            return false;
        }
    }
    return true;
}

static bool readSchemaVersion(sqlite3* db, int64_t& version, std::string& errorMessage) {
    version = 0;
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, "PRAGMA schema_version", -1, &stmt, nullptr) != SQLITE_OK) {
        errorMessage = "Failed to read schema_version";
        return false;
    }
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        version = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return true;
}

struct TableSchemaCacheEntry {
    int64_t schemaVersion = 0;
    std::unordered_set<std::string> columns;
};

static std::mutex& schemaCacheMutex() {
    static std::mutex mutex;
    return mutex;
}

static std::unordered_map<std::string, TableSchemaCacheEntry>& schemaCache() {
    static std::unordered_map<std::string, TableSchemaCacheEntry> cache;
    return cache;
}

static std::string schemaCacheKey(sqlite3*, const std::string& table) {
    return table;
}

static bool loadTableColumns(sqlite3* db, const std::string& table,
                             std::unordered_set<std::string>*& outColumns, std::string& errorMessage,
                             bool forceReload = false) {
    int64_t schemaVersion = 0;
    if (!readSchemaVersion(db, schemaVersion, errorMessage)) {
        return false;
    }

    const std::string cacheKey = schemaCacheKey(db, table);
    if (!forceReload) {
        const std::lock_guard<std::mutex> lock(schemaCacheMutex());
        auto& cache = schemaCache();
        auto it = cache.find(cacheKey);
        if (it != cache.end() && it->second.schemaVersion == schemaVersion) {
            outColumns = &it->second.columns;
            return true;
        }
    }

    std::string pragma = "PRAGMA table_info(" + quoteIdentifier(table) + ")";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, pragma.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        errorMessage = "Failed to prepare table_info pragma";
        return false;
    }

    std::unordered_set<std::string> columns;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* name = sqlite3_column_text(stmt, 1);
        if (name) {
            columns.emplace(reinterpret_cast<const char*>(name));
        }
    }
    sqlite3_finalize(stmt);

    if (columns.empty()) {
        errorMessage = "Failed to load table schema for " + table;
        return false;
    }

    {
        const std::lock_guard<std::mutex> lock(schemaCacheMutex());
        auto& cache = schemaCache();
        cache[cacheKey] = {schemaVersion, std::move(columns)};
        outColumns = &cache[cacheKey].columns;
    }
    return true;
}

static bool applyRowObject(sqlite3* db, const std::string& table, const JsonValue& rowValue,
                           std::string& errorMessage) {
    if (rowValue.type != JsonValue::Type::Object) {
        return true;
    }

    std::unordered_set<std::string>* allowedColumns = nullptr;
    if (!loadTableColumns(db, table, allowedColumns, errorMessage)) {
        return false;
    }

    auto buildKeys = [&rowValue, &allowedColumns](int& missingCount) {
        std::vector<std::string> keys;
        keys.reserve(rowValue.objectValue.size());
        for (const auto& kv : rowValue.objectValue) {
            if (allowedColumns->find(kv.first) != allowedColumns->end()) {
                keys.push_back(kv.first);
            } else {
                missingCount++;
            }
        }
        return keys;
    };

    int missingCount = 0;
    std::vector<std::string> keys = buildKeys(missingCount);
    if (missingCount > 0) {
        if (!loadTableColumns(db, table, allowedColumns, errorMessage, true)) {
            return false;
        }
        missingCount = 0;
        keys = buildKeys(missingCount);
    }
    if (keys.empty()) {
        errorMessage = "No matching columns for table " + table;
        return false;
    }
    std::sort(keys.begin(), keys.end());

    if (allowedColumns->find("id") == allowedColumns->end()) {
        errorMessage = "Table " + table + " missing id column";
        return false;
    }
    if (std::find(keys.begin(), keys.end(), "id") == keys.end()) {
        errorMessage = "Row missing id for table " + table;
        return false;
    }

    std::string columns;
    std::string placeholders;
    columns.reserve(keys.size() * 8);
    placeholders.reserve(keys.size() * 2);
    for (size_t i = 0; i < keys.size(); i++) {
        if (i) {
            columns += ",";
            placeholders += ",";
        }
        columns += quoteIdentifier(keys[i]);
        placeholders += "?";
    }

    std::string sql = "INSERT OR REPLACE INTO " + quoteIdentifier(table) +
                      " (" + columns + ") VALUES (" + placeholders + ")";

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        errorMessage = "Failed to prepare INSERT OR REPLACE";
        return false;
    }

    for (size_t i = 0; i < keys.size(); i++) {
        const auto& value = rowValue.objectValue.at(keys[i]);
        if (!bindValue(stmt, static_cast<int>(i + 1), value)) {
            sqlite3_finalize(stmt);
            errorMessage = "Failed to bind value";
            return false;
        }
    }

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        errorMessage = "Failed to execute INSERT OR REPLACE";
        return false;
    }
    sqlite3_finalize(stmt);
    return true;
}

static const JsonValue* findObjectField(const JsonValue& object, const std::string& key) {
    auto it = object.objectValue.find(key);
    if (it == object.objectValue.end()) {
        return nullptr;
    }
    return &it->second;
}

static bool readStringField(const JsonValue& object, const std::string& key, std::string& out) {
    const JsonValue* value = findObjectField(object, key);
    if (!value || value->type != JsonValue::Type::String) {
        return false;
    }
    out = value->stringValue;
    return true;
}

static bool readBoolField(const JsonValue& object, const std::string& key, bool& out) {
    const JsonValue* value = findObjectField(object, key);
    if (!value || value->type != JsonValue::Type::Bool) {
        return false;
    }
    out = value->boolValue;
    return true;
}

static bool readStringOrNumberField(const JsonValue& object, const std::string& key, std::string& out) {
    const JsonValue* value = findObjectField(object, key);
    if (!value) {
        return false;
    }
    if (value->type == JsonValue::Type::String) {
        out = value->stringValue;
        return true;
    }
    if (value->type == JsonValue::Type::Number) {
        out = value->numberValue;
        return true;
    }
    return false;
}

static const JsonValue* findRowPayload(const JsonValue& entry) {
    if (const JsonValue* row = findObjectField(entry, "row")) {
        return row;
    }
    if (const JsonValue* row = findObjectField(entry, "record")) {
        return row;
    }
    if (const JsonValue* row = findObjectField(entry, "data")) {
        return row;
    }
    return nullptr;
}

static bool extractSequenceId(const JsonValue& entry, std::string& out) {
    if (readStringOrNumberField(entry, "sequenceId", out) ||
        readStringOrNumberField(entry, "sequence_id", out) ||
        readStringOrNumberField(entry, "sequence", out)) {
        return true;
    }

    const JsonValue* row = findRowPayload(entry);
    if (row && row->type == JsonValue::Type::Object) {
        if (readStringOrNumberField(*row, "sequenceId", out) ||
            readStringOrNumberField(*row, "sequence_id", out) ||
            readStringOrNumberField(*row, "sequence", out)) {
            return true;
        }
    }

    return false;
}

static bool extractDeleteFlag(const JsonValue& entry, bool& isDeleted) {
    if (readBoolField(entry, "deleted", isDeleted)) {
        return true;
    }
    if (readBoolField(entry, "isDeleted", isDeleted)) {
        return true;
    }
    if (readBoolField(entry, "is_deleted", isDeleted)) {
        return true;
    }
    std::string type;
    if (readStringField(entry, "type", type) || readStringField(entry, "op", type) || readStringField(entry, "operation", type)) {
        if (type == "delete" || type == "deleted") {
            isDeleted = true;
            return true;
        }
        if (type == "upsert" || type == "insert" || type == "update") {
            isDeleted = false;
            return true;
        }
    }
    return false;
}

static bool extractRowFromEntry(const JsonValue& entry, JsonValue& outRow) {
    const JsonValue* row = findRowPayload(entry);
    if (row) {
        outRow = *row;
        return true;
    }

    if (entry.type != JsonValue::Type::Object) {
        return false;
    }
    outRow.type = JsonValue::Type::Object;
    outRow.objectValue.clear();
    for (const auto& kv : entry.objectValue) {
        const std::string& key = kv.first;
        if (key == "table" || key == "tableName" || key == "deleted" || key == "isDeleted" ||
            key == "is_deleted" || key == "type" || key == "op" || key == "operation") {
            continue;
        }
        outRow.objectValue.emplace(key, kv.second);
    }
    return true;
}

static bool extractDeleteId(const JsonValue& entry, const JsonValue* row, JsonValue& outId) {
    if (row && row->type == JsonValue::Type::Object) {
        const JsonValue* idValue = findObjectField(*row, "id");
        if (idValue) {
            outId = *idValue;
            return true;
        }
    }
    if (entry.type == JsonValue::Type::Object) {
        const JsonValue* idValue = findObjectField(entry, "id");
        if (idValue) {
            outId = *idValue;
            return true;
        }
    }
    return false;
}

static bool setLocalStorage(sqlite3* db, const std::string& key, const std::string& value, std::string& errorMessage) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "INSERT OR REPLACE INTO local_storage (key, value) VALUES (?, ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        errorMessage = "Failed to prepare local_storage insert";
        return false;
    }
    if (sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        errorMessage = "Failed to bind local_storage values";
        return false;
    }
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        errorMessage = "Failed to write local_storage";
        return false;
    }
    sqlite3_finalize(stmt);
    return true;
}

static bool applyDeletes(sqlite3* db, const std::string& table, const JsonValue& rows, std::string& errorMessage) {
    if (rows.type != JsonValue::Type::Array || rows.arrayValue.empty()) {
        return true;
    }
    const int chunkSize = 900;
    size_t total = rows.arrayValue.size();
    for (size_t offset = 0; offset < total; offset += chunkSize) {
        size_t end = std::min(total, offset + chunkSize);
        std::string placeholders;
        for (size_t i = offset; i < end; i++) {
            if (i > offset) placeholders += ",";
            placeholders += "?";
        }
        std::string sql = "DELETE FROM " + quoteIdentifier(table) + " WHERE id IN (" + placeholders + ")";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            errorMessage = "Failed to prepare DELETE";
            return false;
        }
        int bindIndex = 1;
        for (size_t i = offset; i < end; i++) {
            const JsonValue& value = rows.arrayValue[i];
            if (!bindValue(stmt, bindIndex++, value)) {
                sqlite3_finalize(stmt);
                errorMessage = "Failed to bind delete id";
                return false;
            }
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            errorMessage = "Failed to execute DELETE";
            return false;
        }
        sqlite3_finalize(stmt);
    }
    return true;
}

} // namespace

bool applySyncPayload(sqlite3* db, const std::string& payload, std::string& errorMessage) {
    if (!db) {
        errorMessage = "SQLite db is null";
        return false;
    }
    JsonValue root;
    if (!parseJsonWithSimdjson(payload, root, errorMessage)) {
        if (errorMessage.empty()) {
            errorMessage = "Failed to parse JSON";
        }
        return false;
    }

    if (root.type != JsonValue::Type::Array) {
        errorMessage = "Invalid JSON root";
        return false;
    }

    if (!execSql(db, "BEGIN IMMEDIATE", errorMessage)) {
        return false;
    }

    std::unordered_map<std::string, JsonValue> deletesByTable;
    std::string maxSequenceId;
    for (const auto& entry : root.arrayValue) {
        if (entry.type != JsonValue::Type::Object) {
            continue;
        }
        std::string table;
        if (!readStringField(entry, "table", table) && !readStringField(entry, "tableName", table)) {
            errorMessage = "Missing table name in row entry";
            execSql(db, "ROLLBACK", errorMessage);
            return false;
        }

        bool isDeleted = false;
        extractDeleteFlag(entry, isDeleted);

        std::string sequenceId;
        if (extractSequenceId(entry, sequenceId) && !sequenceId.empty()) {
            if (sequenceId > maxSequenceId) {
                maxSequenceId = sequenceId;
            }
        }

        JsonValue rowPayload;
        const JsonValue* rowPtr = findRowPayload(entry);
        if (!rowPtr) {
            extractRowFromEntry(entry, rowPayload);
            rowPtr = &rowPayload;
        }

        if (isDeleted) {
            JsonValue deleteId;
            if (!extractDeleteId(entry, rowPtr, deleteId)) {
                errorMessage = "Missing id for delete entry";
                execSql(db, "ROLLBACK", errorMessage);
                return false;
            }
            JsonValue& deleteArray = deletesByTable[table];
            if (deleteArray.type != JsonValue::Type::Array) {
                deleteArray.type = JsonValue::Type::Array;
                deleteArray.arrayValue.clear();
            }
            deleteArray.arrayValue.emplace_back(std::move(deleteId));
        } else {
            if (!rowPtr || rowPtr->type != JsonValue::Type::Object) {
                errorMessage = "Invalid row payload";
                execSql(db, "ROLLBACK", errorMessage);
                return false;
            }
        if (!applyRowObject(db, table, *rowPtr, errorMessage)) {
            execSql(db, "ROLLBACK", errorMessage);
            return false;
        }
    }
    }

    for (const auto& entry : deletesByTable) {
        if (!applyDeletes(db, entry.first, entry.second, errorMessage)) {
            execSql(db, "ROLLBACK", errorMessage);
            return false;
        }
    }

    if (!maxSequenceId.empty()) {
        // Keep in sync with JS (`SyncManager.refreshPullChangesUrlFromSequenceId`) which reads this key.
        if (!setLocalStorage(db, "__watermelon_last_sequence_id", maxSequenceId, errorMessage)) {
            execSql(db, "ROLLBACK", errorMessage);
            return false;
        }
    }

    if (!execSql(db, "COMMIT", errorMessage)) {
        return false;
    }
    return true;
}

} // namespace watermelondb
