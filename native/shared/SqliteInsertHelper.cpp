#include "SqliteInsertHelper.h"

#include <algorithm>
#include <cmath>

namespace watermelondb {

bool SqliteInsertHelper::bindFieldValue(
    sqlite3* db,
    sqlite3_stmt* stmt,
    int paramIndex,
    const FieldValue& value,
    std::string& errorMessage
) {
    int rc = SQLITE_OK;
    switch (value.type) {
        case FieldValue::Type::NULL_VALUE:
            rc = sqlite3_bind_null(stmt, paramIndex);
            break;
        case FieldValue::Type::INT_VALUE:
            rc = sqlite3_bind_int64(stmt, paramIndex, value.intValue);
            break;
        case FieldValue::Type::REAL_VALUE:
            rc = sqlite3_bind_double(stmt, paramIndex, value.realValue);
            break;
        case FieldValue::Type::TEXT_VALUE:
            rc = sqlite3_bind_text(stmt, paramIndex, value.textValue.c_str(), -1, SQLITE_STATIC);
            break; 
        case FieldValue::Type::BLOB_VALUE:
            rc = sqlite3_bind_blob(
                stmt,
                paramIndex,
                value.blobValue.data(),
                static_cast<int>(value.blobValue.size()),
                SQLITE_STATIC
            );
            break;
    }

    if (rc != SQLITE_OK) {
        errorMessage = sqlite3_errmsg(db);
        return false;
    }

    return true;
}

std::string SqliteInsertHelper::buildColumnsSignature(const std::vector<std::string>& columns) {
    std::string signature;
    signature.reserve(columns.size() * 8);
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) {
            signature += ",";
        }
        signature += columns[i];
    }
    return signature;
}

sqlite3_stmt* SqliteInsertHelper::getCachedMultiRowStatement(
    sqlite3* db,
    const std::string& tableName,
    const std::vector<std::string>& columns,
    const std::string& columnsSignature,
    int rowsInChunk,
    bool shouldCache,
    std::string& errorMessage
) {
    std::string cacheKey = tableName + "|" + columnsSignature + "|" + std::to_string(rowsInChunk);

    if (shouldCache) {
        auto it = statementCache_.find(cacheKey);
        if (it != statementCache_.end()) {
            return it->second;
        }
    }

    std::string columnNames;
    columnNames.reserve(columns.size() * 8);
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) {
            columnNames += ", ";
        }
        columnNames += "\"" + columns[i] + "\"";
    }

    std::string valuesClause;
    for (int rowIdx = 0; rowIdx < rowsInChunk; rowIdx++) {
        if (rowIdx > 0) {
            valuesClause += ", ";
        }
        valuesClause += "(";
        for (size_t colIdx = 0; colIdx < columns.size(); colIdx++) {
            if (colIdx > 0) {
                valuesClause += ", ";
            }
            valuesClause += "?";
        }
        valuesClause += ", 'synced')";
    }

    std::string sql = "INSERT OR IGNORE INTO \"" + tableName + "\" (" + columnNames + ", \"_status\") VALUES " + valuesClause;

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        errorMessage = sqlite3_errmsg(db);
        return nullptr;
    }

    if (shouldCache) {
        statementCache_[cacheKey] = stmt;
    }

    return stmt;
}

bool SqliteInsertHelper::insertRowsMulti(
    sqlite3* db,
    const std::string& tableName,
    const std::vector<std::string>& columns,
    const std::vector<std::vector<FieldValue>>& rows,
    std::string& errorMessage
) {
    if (rows.empty()) {
        return true;
    }

    size_t columnCount = columns.size();
    if (columnCount == 0) {
        return true;
    }

    int maxRowsPerStmt = static_cast<int>(floor(900.0 / static_cast<double>(columnCount)));
    if (maxRowsPerStmt < 1) {
        maxRowsPerStmt = 1;
    }

    std::string columnsSignature = buildColumnsSignature(columns);

    size_t totalRows = rows.size();
    size_t offset = 0;
    while (offset < totalRows) {
        int chunkSize = static_cast<int>(std::min(static_cast<size_t>(maxRowsPerStmt), totalRows - offset));
        bool shouldCache = (chunkSize == maxRowsPerStmt);

        sqlite3_stmt* stmt = getCachedMultiRowStatement(
            db,
            tableName,
            columns,
            columnsSignature,
            chunkSize,
            shouldCache,
            errorMessage
        );
        if (!stmt) {
            return false;
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        int paramIndex = 1;
        static const FieldValue kNullValue = FieldValue::makeNull();
        for (size_t rowIdx = 0; rowIdx < static_cast<size_t>(chunkSize); rowIdx++) {
            const auto& rowValues = rows[offset + rowIdx];
            for (size_t colIdx = 0; colIdx < columnCount; colIdx++) {
                const FieldValue& value = (colIdx < rowValues.size()) ? rowValues[colIdx] : kNullValue;
                if (!bindFieldValue(db, stmt, paramIndex, value, errorMessage)) {
                    if (!shouldCache && stmt) {
                        sqlite3_finalize(stmt);
                    }
                    return false;
                }
                paramIndex++;
            }
        }

        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            errorMessage = sqlite3_errmsg(db);
            if (!shouldCache && stmt) {
                sqlite3_finalize(stmt);
            }
            return false;
        }

        if (!shouldCache && stmt) {
            sqlite3_finalize(stmt);
        }

        offset += static_cast<size_t>(chunkSize);
    }

    return true;
}

bool SqliteInsertHelper::insertBatch(
    sqlite3* db,
    const BatchData& batch,
    std::string& errorMessage
) {
    if (batch.totalRows == 0) {
        return true;
    }

    std::vector<std::string> tableNames;
    tableNames.reserve(batch.tables.size());
    for (const auto& pair : batch.tables) {
        tableNames.push_back(pair.first);
    }
    std::sort(tableNames.begin(), tableNames.end());

    for (const auto& tableName : tableNames) {
        const auto& rows = batch.tables.at(tableName);
        const auto& columns = batch.tableColumns.at(tableName);
        if (!insertRowsMulti(db, tableName, columns, rows, errorMessage)) {
            return false;
        }
    }

    return true;
}

void SqliteInsertHelper::finalizeStatements() {
    for (auto& entry : statementCache_) {
        if (entry.second) {
            sqlite3_finalize(entry.second);
        }
    }
    statementCache_.clear();
}

} // namespace watermelondb
