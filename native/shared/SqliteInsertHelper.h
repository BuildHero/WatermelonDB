#pragma once

#include "SliceImportEngine.h"
#include <sqlite3.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace watermelondb {

class SqliteInsertHelper {
public:
    SqliteInsertHelper() = default;

    bool insertRowsMulti(
        sqlite3* db,
        const std::string& tableName,
        const std::vector<std::string>& columns,
        const std::vector<std::vector<FieldValue>>& rows,
        std::string& errorMessage
    );

    bool insertBatch(
        sqlite3* db,
        const BatchData& batch,
        std::string& errorMessage
    );

    void finalizeStatements();

private:
    std::unordered_map<std::string, sqlite3_stmt*> statementCache_;

    static bool bindFieldValue(
        sqlite3* db,
        sqlite3_stmt* stmt,
        int paramIndex,
        const FieldValue& value,
        std::string& errorMessage
    );

    static std::string buildColumnsSignature(const std::vector<std::string>& columns);

    sqlite3_stmt* getCachedMultiRowStatement(
        sqlite3* db,
        const std::string& tableName,
        const std::vector<std::string>& columns,
        const std::string& columnsSignature,
        int rowsInChunk,
        bool shouldCache,
        std::string& errorMessage
    );
};

} // namespace watermelondb
