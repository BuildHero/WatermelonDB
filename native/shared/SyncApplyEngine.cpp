#include "SyncApplyEngine.h"
#include "JsonUtils.h"

#include <algorithm>
#include <cctype>
#include <unordered_map>
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

static bool applyRows(sqlite3* db, const std::string& table, const JsonValue& rows, std::string& errorMessage) {
    if (rows.type != JsonValue::Type::Array) {
        return true;
    }
    for (const auto& rowValue : rows.arrayValue) {
        if (rowValue.type != JsonValue::Type::Object) {
            continue;
        }
        std::vector<std::string> keys;
        keys.reserve(rowValue.objectValue.size());
        for (const auto& kv : rowValue.objectValue) {
            keys.push_back(kv.first);
        }
        if (keys.empty()) {
            continue;
        }
        std::sort(keys.begin(), keys.end());

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
    }
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

    if (root.type != JsonValue::Type::Object) {
        errorMessage = "Invalid JSON root";
        return false;
    }

    const JsonValue* changes = nullptr;
    auto it = root.objectValue.find("changes");
    if (it != root.objectValue.end()) {
        changes = &it->second;
    } else {
        changes = &root;
    }
    if (!changes || changes->type != JsonValue::Type::Object) {
        errorMessage = "Invalid changes object";
        return false;
    }

    if (!execSql(db, "BEGIN IMMEDIATE", errorMessage)) {
        return false;
    }

    for (const auto& tableEntry : changes->objectValue) {
        const std::string& table = tableEntry.first;
        const JsonValue& tableChanges = tableEntry.second;
        if (tableChanges.type != JsonValue::Type::Object) {
            continue;
        }
        auto createdIt = tableChanges.objectValue.find("created");
        auto updatedIt = tableChanges.objectValue.find("updated");
        auto deletedIt = tableChanges.objectValue.find("deleted");

        if (createdIt != tableChanges.objectValue.end()) {
            if (!applyRows(db, table, createdIt->second, errorMessage)) {
                execSql(db, "ROLLBACK", errorMessage);
                return false;
            }
        }
        if (updatedIt != tableChanges.objectValue.end()) {
            if (!applyRows(db, table, updatedIt->second, errorMessage)) {
                execSql(db, "ROLLBACK", errorMessage);
                return false;
            }
        }
        if (deletedIt != tableChanges.objectValue.end()) {
            if (!applyDeletes(db, table, deletedIt->second, errorMessage)) {
                execSql(db, "ROLLBACK", errorMessage);
                return false;
            }
        }
    }

    if (!execSql(db, "COMMIT", errorMessage)) {
        return false;
    }
    return true;
}

} // namespace watermelondb
