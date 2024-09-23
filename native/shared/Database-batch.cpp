#include "Database.h"

namespace watermelondb {

using platform::consoleError;
using platform::consoleLog;

// old version (Jonathan DiCamillo)
void Database::batch(jsi::Array &operations) {
    auto &rt = getRt();
    beginTransaction();

    std::vector<std::string> addedIds = {};
    std::vector<std::string> removedIds = {};

    try {
        size_t operationsCount = operations.length(rt);
        for (size_t i = 0; i < operationsCount; i++) {
            jsi::Array operation = operations.getValueAtIndex(rt, i).getObject(rt).getArray(rt);
            std::string type = operation.getValueAtIndex(rt, 0).getString(rt).utf8(rt);
            const jsi::String table = operation.getValueAtIndex(rt, 1).getString(rt);

            if (type == "create") {
                std::string id = operation.getValueAtIndex(rt, 2).getString(rt).utf8(rt);
                std::string sql = operation.getValueAtIndex(rt, 3).getString(rt).utf8(rt);
                jsi::Array arguments = operation.getValueAtIndex(rt, 4).getObject(rt).getArray(rt);

                executeUpdate(sql, arguments);
                addedIds.push_back(cacheKey(table.utf8(rt), id));
            } else if (type == "execute") {
                jsi::String sql = operation.getValueAtIndex(rt, 2).getString(rt);
                jsi::Array arguments = operation.getValueAtIndex(rt, 3).getObject(rt).getArray(rt);

                executeUpdate(sql.utf8(rt), arguments);
            } else if (type == "markAsDeleted") {
                const jsi::String id = operation.getValueAtIndex(rt, 2).getString(rt);
                auto args = jsi::Array::createWithElements(rt, id);
                executeUpdate("update `" + table.utf8(rt) + "` set _status='deleted' where id == ?", args);

                removedIds.push_back(cacheKey(table.utf8(rt), id.utf8(rt)));
            } else if (type == "destroyPermanently") {
                const jsi::String id = operation.getValueAtIndex(rt, 2).getString(rt);
                auto args = jsi::Array::createWithElements(rt, id);

                // TODO: What's the behavior if nothing got deleted?
                executeUpdate("delete from `" + table.utf8(rt) + "` where id == ?", args);
                removedIds.push_back(cacheKey(table.utf8(rt), id.utf8(rt)));
            } else {
                throw jsi::JSError(rt, "Invalid operation type");
            }
        }
        commit();
    } catch (const std::exception &ex) {
        rollback();
        throw;
    }

    for (auto const &key : addedIds) {
        markAsCached(key);
    }

    for (auto const &key : removedIds) {
        removeFromCache(key);
    }
}

void Database::batchJSON(jsi::String &&jsiJson) {
    using namespace simdjson;

    auto &rt = getRt();
    const std::lock_guard<std::mutex> lock(mutex_);
    beginTransaction();

    std::vector<std::string> addedIds = {};
    std::vector<std::string> removedIds = {};

    try {
        ondemand::parser parser;
        auto json = padded_string(jsiJson.utf8(rt));
        ondemand::document doc = parser.iterate(json);

        // NOTE: simdjson::ondemand processes forwards-only, hence the weird field enumeration
        // We can't use subscript or backtrack.
        for (ondemand::array operation : doc) {
            int64_t cacheBehavior = 0;
            std::string table;
            std::string sql;
            size_t fieldIdx = 0;
            for (auto field : operation) {
                if (fieldIdx == 0) {
                    cacheBehavior = field;
                } else if (fieldIdx == 1) {
                    if (cacheBehavior != 0) {
                        table = (std::string_view) field;
                    }
                } else if (fieldIdx == 2) {
                    sql = (std::string_view) field;
                } else if (fieldIdx == 3) {
                    ondemand::array argsBatches = field;
                    auto stmt = prepareQuery(sql);
                    SqliteStatement statement(stmt);

                    for (ondemand::array args : argsBatches) {
                        // NOTE: We must capture the ID once first parsed
                        auto id = bindArgsAndReturnId(stmt, args);
                        executeUpdate(stmt);
                        sqlite3_reset(stmt);
                        if (cacheBehavior == 1) {
                            addedIds.push_back(cacheKey(table, id));
                        } else if (cacheBehavior == -1) {
                            removedIds.push_back(cacheKey(table, id));
                        }
                    }
                }
                fieldIdx++;
            }
        }

        commit();
    } catch (const std::exception &ex) {
        rollback();
        throw;
    }

    for (auto const &key : addedIds) {
        markAsCached(key);
    }

    for (auto const &key : removedIds) {
        removeFromCache(key);
    }
}

}
