#include <string>

namespace watermelondb::platform {

void consoleLog(std::string) {}

void consoleError(std::string) {}

void initializeSqlite() {}

std::string resolveDatabasePath(std::string path) {
    return path;
}

} // namespace watermelondb::platform
