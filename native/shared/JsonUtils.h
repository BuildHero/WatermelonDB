#pragma once

#include <string>

namespace watermelondb {
namespace json_utils {

// Escapes a string for use in JSON output
// Handles common escape sequences: \", \\, \n, \r, \t
inline std::string escapeJsonString(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        switch (c) {
            case '\"': escaped += "\\\""; break;
            case '\\': escaped += "\\\\"; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default: escaped += c; break;
        }
    }
    return escaped;
}

} // namespace json_utils
} // namespace watermelondb
