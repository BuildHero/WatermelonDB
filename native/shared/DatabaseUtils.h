//
//  DatabaseUtils.hpp
//  WatermelonDB
//
//  Created by BuildOpsLA27 on 10/4/24.
//

#ifndef DatabaseUtils_hpp
#define DatabaseUtils_hpp

#import <jsi/jsi.h>
#import <unordered_map>
#import <unordered_set>
#import <sqlite3.h>

#import "Sqlite.h"

using namespace facebook;

namespace watermelondb {

sqlite3_stmt* getStmt(jsi::Runtime &rt, sqlite3* db, std::string sql, const jsi::Array &arguments);

void finalizeStmt(sqlite3_stmt* stmt);

// Finalizes a prepared statement on scope exit, including when the scope is left by
// a thrown jsi::JSError. Callers used to finalize only after their row loop, so any
// throw from stepping/decoding (or an explicit JSError mid-loop) leaked the
// statement — and a leaked statement holds both memory and an open read
// transaction, so a repeatedly-failing query could manufacture a real
// SQLITE_NOMEM on top of whatever the original failure was.
class StmtGuard {
public:
    explicit StmtGuard(sqlite3_stmt *stmt) : stmt_(stmt) {}
    ~StmtGuard() {
        if (stmt_ != nullptr) {
            sqlite3_finalize(stmt_);
        }
    }
    StmtGuard(const StmtGuard &) = delete;
    StmtGuard &operator=(const StmtGuard &) = delete;

    sqlite3_stmt *get() const { return stmt_; }

    // Transfers ownership to the caller: the guard no longer finalizes on
    // destruction. Used by getStmt() to hand back a fully-prepared statement
    // on the success path while still finalizing on every throwing path.
    sqlite3_stmt *release() {
        sqlite3_stmt *stmt = stmt_;
        stmt_ = nullptr;
        return stmt;
    }

private:
    sqlite3_stmt *stmt_;
};

jsi::Array arrayFromStd(jsi::Runtime &rt, std::vector<jsi::Value> &vector);

jsi::Object resultDictionary(jsi::Runtime &rt, sqlite3_stmt *statement);

bool getNextRowOrTrue(jsi::Runtime &rt, sqlite3_stmt *stmt);

}
#endif /* DatabaseUtils_hpp */
