#pragma once

#include <sqlite3.h>
#include <string>

namespace watermelondb {

bool applySyncPayload(sqlite3* db, const std::string& payload, std::string& errorMessage);

} // namespace watermelondb
