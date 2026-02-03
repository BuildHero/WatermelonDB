#pragma once

#include <functional>
#include <string>
#include <unordered_map>

namespace watermelondb {
namespace platform {

struct HttpRequest {
    std::string method;
    std::string url;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
    int timeoutMs = 30000;
};

struct HttpResponse {
    int statusCode = 0;
    std::string body;
    std::string errorMessage;
};

void httpRequest(const HttpRequest& request, std::function<void(const HttpResponse&)> onComplete);

} // namespace platform
} // namespace watermelondb
