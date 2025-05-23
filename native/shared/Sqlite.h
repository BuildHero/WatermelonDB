#pragma once
#if defined __cplusplus

#import <string>
#import <sqlite3.h>

namespace watermelondb {

// Lightweight wrapper for handling sqlite3 lifetime
class SqliteDb {
public:
    SqliteDb(std::string path);
    ~SqliteDb();

    sqlite3 *sqlite;

    SqliteDb &operator=(const SqliteDb &) = delete;
    SqliteDb(const SqliteDb &) = delete;
};

class SqliteStatement {
public:
    SqliteStatement(sqlite3_stmt *statement);
    ~SqliteStatement();

    sqlite3_stmt *stmt;

    void reset();
};

} // namespace watermelondb

#endif
