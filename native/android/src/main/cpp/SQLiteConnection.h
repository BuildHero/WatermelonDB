// Shared SQLiteConnection definition for Android native code.
#pragma once

#include <sqlite3.h>
#include <cstring>
#include <cstdlib>

struct SQLiteConnection {
    sqlite3* const db;
    const int openFlags;
    char* path;
    char* label;

    volatile bool canceled;

    SQLiteConnection(sqlite3* db, int openFlags, const char* path_, const char* label_)
        : db(db), openFlags(openFlags), path(nullptr), label(nullptr), canceled(false) {
        path = path_ ? strdup(path_) : nullptr;
        label = label_ ? strdup(label_) : nullptr;
    }

    ~SQLiteConnection() {
        if (path) {
            free(path);
        }
        if (label) {
            free(label);
        }
    }
};
