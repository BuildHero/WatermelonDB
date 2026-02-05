#include "SyncEngine.h"
#include "JsonUtils.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <thread>
#include <vector>

#if __has_include(<simdjson.h>)
#include <simdjson.h>
#elif __has_include("simdjson.h")
#include "simdjson.h"
#else
#error "simdjson headers not found. Please add @nozbe/simdjson or provide simdjson headers."
#endif

namespace watermelondb {

namespace {

bool isUnreservedUrlChar(unsigned char c) {
    return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~';
}

std::string urlEncode(const std::string& value) {
    std::ostringstream encoded;
    encoded.fill('0');
    encoded << std::hex << std::uppercase;
    for (unsigned char c : value) {
        if (isUnreservedUrlChar(c)) {
            encoded << static_cast<char>(c);
        } else {
            encoded << '%' << std::setw(2) << static_cast<int>(c);
        }
    }
    return encoded.str();
}

struct CursorValue {
    std::string value;
    bool encoded = false;
};

std::string buildUrlWithCursor(const std::string& baseUrl, const std::string& cursor, bool encoded) {
    const std::string encodedCursor = encoded ? cursor : urlEncode(cursor);
    const size_t queryPos = baseUrl.find('?');
    const std::string base = queryPos == std::string::npos ? baseUrl : baseUrl.substr(0, queryPos);
    const std::string query = queryPos == std::string::npos ? "" : baseUrl.substr(queryPos + 1);

    std::vector<std::string> parts;
    bool replaced = false;
    size_t start = 0;
    while (start <= query.size()) {
        const size_t end = query.find('&', start);
        const std::string part = query.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (!part.empty()) {
            if (part.rfind("cursor=", 0) == 0) {
                parts.emplace_back("cursor=" + encodedCursor);
                replaced = true;
            } else {
                parts.emplace_back(part);
            }
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }

    if (!replaced) {
        parts.emplace_back("cursor=" + encodedCursor);
    }

    std::ostringstream rebuilt;
    rebuilt << base;
    if (!parts.empty()) {
        rebuilt << '?';
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) {
                rebuilt << '&';
            }
            rebuilt << parts[i];
        }
    }
    return rebuilt.str();
}

bool appendJsonValue(const simdjson::dom::element& element, std::string& out);

bool appendJsonString(const std::string& value, std::string& out) {
    out += "\"";
    out += json_utils::escapeJsonString(value);
    out += "\"";
    return true;
}

bool appendJsonValue(const simdjson::dom::element& element, std::string& out) {
    switch (element.type()) {
        case simdjson::dom::element_type::OBJECT: {
            simdjson::dom::object obj;
            if (element.get(obj)) {
                return false;
            }
            out += "{";
            bool first = true;
            for (auto field : obj) {
                if (!first) {
                    out += ",";
                }
                first = false;
                std::string_view key = field.key;
                appendJsonString(std::string(key), out);
                out += ":";
                if (!appendJsonValue(field.value, out)) {
                    return false;
                }
            }
            out += "}";
            return true;
        }
        case simdjson::dom::element_type::ARRAY: {
            simdjson::dom::array arr;
            if (element.get(arr)) {
                return false;
            }
            out += "[";
            bool first = true;
            for (auto value : arr) {
                if (!first) {
                    out += ",";
                }
                first = false;
                if (!appendJsonValue(value, out)) {
                    return false;
                }
            }
            out += "]";
            return true;
        }
        case simdjson::dom::element_type::STRING: {
            std::string_view value;
            if (element.get(value)) {
                return false;
            }
            return appendJsonString(std::string(value), out);
        }
        case simdjson::dom::element_type::INT64: {
            int64_t value = 0;
            if (element.get(value)) {
                return false;
            }
            out += std::to_string(value);
            return true;
        }
        case simdjson::dom::element_type::UINT64: {
            uint64_t value = 0;
            if (element.get(value)) {
                return false;
            }
            out += std::to_string(value);
            return true;
        }
        case simdjson::dom::element_type::DOUBLE: {
            double value = 0;
            if (element.get(value)) {
                return false;
            }
            out += std::to_string(value);
            return true;
        }
        case simdjson::dom::element_type::BOOL: {
            bool value = false;
            if (element.get(value)) {
                return false;
            }
            out += value ? "true" : "false";
            return true;
        }
        case simdjson::dom::element_type::NULL_VALUE:
            out += "null";
            return true;
    }
    return false;
}

bool extractNextCursor(const std::string& body, CursorValue& cursorOut) {
    cursorOut.value.clear();
    cursorOut.encoded = false;
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element doc = parser.parse(body);
        if (doc.type() != simdjson::dom::element_type::OBJECT) {
            return false;
        }
        simdjson::dom::object obj;
        if (doc.get(obj)) {
            return false;
        }
        auto nextField = obj["next"];
        if (nextField.error() == simdjson::NO_SUCH_FIELD) {
            return false;
        }
        if (nextField.error()) {
            return false;
        }
        simdjson::dom::element nextValue = nextField.value();
        if (nextValue.is_null()) {
            return false;
        }
        if (nextValue.is_string()) {
            std::string_view str;
            if (nextValue.get(str)) {
                return false;
            }
            cursorOut.value = std::string(str);
            cursorOut.encoded = true;
            return !cursorOut.value.empty();
        }
        std::string json;
        if (!appendJsonValue(nextValue, json)) {
            return false;
        }
        cursorOut.value = std::move(json);
        cursorOut.encoded = false;
        return !cursorOut.value.empty();
    } catch (...) {
        return false;
    }
}

} // namespace

SyncEngine::SyncEngine() = default;

void SyncEngine::setEventCallback(EventCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
        return;
    }
    eventCallback_ = std::move(callback);
}

void SyncEngine::setApplyCallback(ApplyCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
        return;
    }
    applyCallback_ = std::move(callback);
}

void SyncEngine::setAuthTokenRequestCallback(AuthTokenRequestCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (shutdown_) {
        return;
    }

    authTokenRequestCallback_ = std::move(callback);
}

void SyncEngine::setPushChangesCallback(PushChangesCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (shutdown_) {
        return;
    }

    pushChangesCallback_ = std::move(callback);
}

void SyncEngine::configure(const std::string& configJson) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
        return;
    }
    configJson_ = configJson;
    pullEndpointUrl_ = getJsonStringValue(configJson_, "pullEndpointUrl");
    socketioUrl_ = getJsonStringValue(configJson_, "socketioUrl");
    timeoutMs_ = getJsonIntValue(configJson_, "timeoutMs", 30000);
    maxRetries_ = std::max(0, getJsonIntValue(configJson_, "maxRetries", 3));
    maxAuthRetries_ = std::max(0, getJsonIntValue(configJson_, "maxAuthRetries", 3));
    retryInitialMs_ = std::max(0, getJsonIntValue(configJson_, "retryInitialMs", 1000));
    retryMaxMs_ = std::max(retryInitialMs_, getJsonIntValue(configJson_, "retryMaxMs", 30000));
    stateJson_ = "{\"state\":\"configured\"}";
    emitLocked(stateJson_);
}

void SyncEngine::setPullEndpointUrl(const std::string& url) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (shutdown_) {
        return;
    }

    pullEndpointUrl_ = url;
}

void SyncEngine::setAuthToken(const std::string& token) {
    bool shouldRestart = false;
    CompletionCallback completion;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (shutdown_) {
            return;
        }

        authToken_ = token;

        authRequestInFlight_ = false;
        authRetryCount_ = 0; // Reset auth retry count on successful token

        if (!syncInFlight_ && stateJson_ == "{\"state\":\"auth_required\"}") {
            shouldRestart = true;
            completion = std::move(completionCallback_);
        }
    }
    if (shouldRestart) {
        if (completion) {
            startWithCompletion("auth_token_updated", std::move(completion));
        } else {
            start("auth_token_updated");
        }
    }
}

void SyncEngine::clearAuthToken() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
        return;
    }
    authToken_.clear();
    authRequestInFlight_ = false;
}

void SyncEngine::requestAuthToken() {
    AuthTokenRequestCallback callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (shutdown_ || authRequestInFlight_) {
            return;
        }

        authRequestInFlight_ = true;

        callback = authTokenRequestCallback_;
    }

    if (callback) {
        callback();
    }
}

void SyncEngine::start(const std::string& reason) {
    startWithCompletion(reason, nullptr);
}

void SyncEngine::startWithCompletion(const std::string& reason, CompletionCallback completion) {
    bool shouldStart = false;
    int64_t syncId = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncInFlight_) {
            pendingReason_ = reason;
            pendingCompletionCallback_ = std::move(completion);
            emitLocked(std::string("{\"type\":\"sync_queued\",\"reason\":\"") + json_utils::escapeJsonString(reason) + "\"}");
            return;
        }
        syncInFlight_ = true;
        retryScheduled_ = false;
        retryCount_ = 0;
        authRetryCount_ = 0; // Reset auth retry count on new sync
        currentReason_ = reason;
        if (completion) {
            completionCallback_ = std::move(completion);
        }
        const bool resumeFromAuth = (stateJson_ == "{\"state\":\"auth_required\"}") && !currentPullUrl_.empty();
        if (!resumeFromAuth) {
            currentRequestId_ = platform::generateRequestId();
            currentPullUrl_ = pullEndpointUrl_;
        } else if (currentRequestId_.empty()) {
            currentRequestId_ = platform::generateRequestId();
        }
        stateJson_ = "{\"state\":\"sync_requested\"}";
        emitLocked("{\"type\":\"state\",\"state\":\"sync_requested\"}");
        emitLocked(std::string("{\"type\":\"sync_start\",\"reason\":\"") + json_utils::escapeJsonString(reason) + "\"}");
        syncId_++;
        syncId = syncId_;
        shouldStart = true;
    }
    if (shouldStart) {
        dispatchRequest(syncId, false);
    }
}

std::string SyncEngine::stateJson() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stateJson_;
}

void SyncEngine::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
    eventCallback_ = nullptr;
    applyCallback_ = nullptr;
    syncInFlight_ = false;
    retryScheduled_ = false;
    retryCount_ = 0;
    pendingReason_.clear();
    currentReason_.clear();
    currentRequestId_.clear();
    currentPullUrl_.clear();
    stateJson_ = "{\"state\":\"idle\"}";
    syncId_++;
}

void SyncEngine::emitLocked(const std::string& eventJson) {
    if (eventCallback_) {
        eventCallback_(eventJson);
    }
}

void SyncEngine::dispatchRequest(int64_t syncId, bool isRetry) {
    std::string pullEndpointUrl;
    std::string authToken;
    std::string requestId;
    int timeoutMs = 30000;
    int attempt = 1;
    AuthTokenRequestCallback authRequest;
    bool shouldRequestAuth = false;
    bool missingPullEndpointUrl = false;
    CompletionCallback completion;
    CompletionCallback pendingCompletion;
    std::string completionError;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }
        pullEndpointUrl = currentPullUrl_.empty() ? pullEndpointUrl_ : currentPullUrl_;
        authToken = authToken_;
        requestId = currentRequestId_;
        timeoutMs = timeoutMs_;
        attempt = retryCount_ + 1;
        if (requestId.empty()) {
            currentRequestId_ = platform::generateRequestId();
            requestId = currentRequestId_;
        }

        if (pullEndpointUrl.empty()) {
            emitLocked("{\"type\":\"error\",\"message\":\"Missing sync pullEndpointUrl\"}");
            stateJson_ = "{\"state\":\"error\"}";
            emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
            syncInFlight_ = false;
            retryScheduled_ = false;
            currentRequestId_.clear();
            currentPullUrl_.clear();
            completion = std::move(completionCallback_);
            pendingCompletion = std::move(pendingCompletionCallback_);
            pendingCompletionCallback_ = nullptr;
            pendingReason_.clear();
            missingPullEndpointUrl = true;
        }

        if (!missingPullEndpointUrl && authToken.empty() && authTokenRequestCallback_) {
            if (authRetryCount_ >= maxAuthRetries_) {
                // Auth retries exhausted
                emitLocked("{\"type\":\"auth_failed\",\"message\":\"Max auth retries exceeded\"}");
                emitLocked("{\"type\":\"error\",\"message\":\"Max auth retries exceeded\"}");
                stateJson_ = "{\"state\":\"auth_failed\"}";
                emitLocked("{\"type\":\"state\",\"state\":\"auth_failed\"}");
                syncInFlight_ = false;
                retryScheduled_ = false;
                retryCount_ = 0;
                currentRequestId_.clear();
                currentPullUrl_.clear();
                completion = std::move(completionCallback_);
                pendingCompletion = std::move(pendingCompletionCallback_);
                pendingCompletionCallback_ = nullptr;
                pendingReason_.clear();
                completionError = "Max auth retries exceeded";
                missingPullEndpointUrl = true; // Reuse this flag to trigger early return
            } else {
                stateJson_ = "{\"state\":\"auth_required\"}";
                emitLocked("{\"type\":\"auth_required\"}");
                emitLocked("{\"type\":\"state\",\"state\":\"auth_required\"}");
                syncInFlight_ = false;
                retryScheduled_ = false;
                retryCount_ = 0;
                if (!authRequestInFlight_) {
                    authRequestInFlight_ = true;
                    authRetryCount_++;
                    authRequest = authTokenRequestCallback_;
                }
                shouldRequestAuth = true;
            }
        }

        if (!shouldRequestAuth) {
            stateJson_ = "{\"state\":\"syncing\"}";
            emitLocked("{\"type\":\"state\",\"state\":\"syncing\"}");
            emitLocked(std::string("{\"type\":\"phase\",\"phase\":\"pull\",\"attempt\":") + std::to_string(attempt) + "}");
            if (isRetry) {
                emitLocked(std::string("{\"type\":\"sync_retry\",\"attempt\":") + std::to_string(attempt) + "}");
            }
        }
    }

    if (missingPullEndpointUrl) {
        const std::string& errorMsg = completionError.empty() ? "Missing sync pullEndpointUrl" : completionError;
        if (completion) {
            completion(false, errorMsg);
        }
        if (pendingCompletion) {
            pendingCompletion(false, errorMsg);
        }
        return;
    }
    if (shouldRequestAuth) {
        if (authRequest) {
            authRequest();
        }
        return;
    }

    platform::HttpRequest request;
    request.method = "GET";
    request.url = pullEndpointUrl;
    request.timeoutMs = timeoutMs;
    if (!authToken.empty()) {
        request.headers["Authorization"] = authToken;
    }
    if (!requestId.empty()) {
        request.headers["X-Request-Id"] = requestId;
    }
    // Discrete marker to identify native sync engine traffic in server logs
    request.headers["x-sync-engine"] = "1";

    platform::httpRequest(request, [self = shared_from_this(), syncId](const platform::HttpResponse& response) {
        self->handleHttpResponse(syncId, response);
    });
}

void SyncEngine::handleHttpResponse(int64_t syncId, const platform::HttpResponse& response) {
    AuthTokenRequestCallback authRequest;
    CompletionCallback completion;
    CompletionCallback pendingCompletion;
    bool shouldReturn = false;
    bool authRequired = false;
    std::string completionError;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }

        if (!response.errorMessage.empty()) {
            if (scheduleRetryLocked(syncId, response.statusCode, response.errorMessage)) {
                return;
            }
            emitLocked(std::string("{\"type\":\"error\",\"message\":\"") +
                       json_utils::escapeJsonString(response.errorMessage) + "\"}");
            stateJson_ = "{\"state\":\"error\"}";
            emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
            syncInFlight_ = false;
            retryScheduled_ = false;
            retryCount_ = 0;
            currentRequestId_.clear();
            currentPullUrl_.clear();
            completion = std::move(completionCallback_);
            pendingCompletion = std::move(pendingCompletionCallback_);
            pendingCompletionCallback_ = nullptr;
            pendingReason_.clear();
            completionError = response.errorMessage;
            shouldReturn = true;
        }

        if (response.statusCode == 401 || response.statusCode == 403) {
            if (authRetryCount_ >= maxAuthRetries_) {
                // Auth retries exhausted
                emitLocked("{\"type\":\"auth_failed\",\"message\":\"Max auth retries exceeded\"}");
                emitLocked("{\"type\":\"error\",\"message\":\"Max auth retries exceeded\"}");
                stateJson_ = "{\"state\":\"auth_failed\"}";
                emitLocked("{\"type\":\"state\",\"state\":\"auth_failed\"}");
                syncInFlight_ = false;
                retryScheduled_ = false;
                retryCount_ = 0;
                currentRequestId_.clear();
                currentPullUrl_.clear();
                completion = std::move(completionCallback_);
                pendingCompletion = std::move(pendingCompletionCallback_);
                pendingCompletionCallback_ = nullptr;
                pendingReason_.clear();
                completionError = "Max auth retries exceeded";
                shouldReturn = true;
            } else {
                stateJson_ = "{\"state\":\"auth_required\"}";
                emitLocked("{\"type\":\"auth_required\"}");
                emitLocked("{\"type\":\"state\",\"state\":\"auth_required\"}");
                syncInFlight_ = false;
                retryScheduled_ = false;
                retryCount_ = 0;
                if (!authRequestInFlight_) {
                    authRequestInFlight_ = true;
                    authRetryCount_++;
                    authRequest = authTokenRequestCallback_;
                }
                authRequired = true;
                shouldReturn = true;
            }
        } else if (response.statusCode >= 400) {
            if (scheduleRetryLocked(syncId, response.statusCode, std::string("HTTP ") + std::to_string(response.statusCode))) {
                return;
            }
            emitLocked(std::string("{\"type\":\"error\",\"message\":\"HTTP ") +
                       std::to_string(response.statusCode) + "\"}");
            stateJson_ = "{\"state\":\"error\"}";
            emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
            syncInFlight_ = false;
            retryScheduled_ = false;
            retryCount_ = 0;
            currentRequestId_.clear();
            currentPullUrl_.clear();
            completion = std::move(completionCallback_);
            pendingCompletion = std::move(pendingCompletionCallback_);
            pendingCompletionCallback_ = nullptr;
            pendingReason_.clear();
            completionError = std::string("HTTP ") + std::to_string(response.statusCode);
            shouldReturn = true;
        } else {
            emitLocked(std::string("{\"type\":\"http\",\"phase\":\"pull\",\"status\":") +
                       std::to_string(response.statusCode) + "}");
        }
    }
    if (shouldReturn) {
        if (authRequired) {
            if (authRequest) {
                authRequest();
            }
            return;
        }
        if (completion) {
            completion(false, completionError);
        }
        if (pendingCompletion) {
            pendingCompletion(false, completionError);
        }
        return;
    }
    
    if (authRequest) {
        authRequest();
    }

    ApplyCallback applyCb;
    PushChangesCallback pushChangesCb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }
        applyCb = applyCallback_;
        pushChangesCb = pushChangesCallback_;
    }

    std::string applyError;

    if (applyCb) {
        if (!applyCb(response.body, applyError)) {
            CompletionCallback completionToCall;
            CompletionCallback pendingToCall;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                emitLocked(std::string("{\"type\":\"error\",\"message\":\"") +
                           json_utils::escapeJsonString(applyError) + "\"}");
                stateJson_ = "{\"state\":\"error\"}";
                emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
                syncInFlight_ = false;
                retryScheduled_ = false;
                retryCount_ = 0;
                currentRequestId_.clear();
                currentPullUrl_.clear();
                completionToCall = std::move(completionCallback_);
                pendingToCall = std::move(pendingCompletionCallback_);
                pendingCompletionCallback_ = nullptr;
                pendingReason_.clear();
            }
            if (completionToCall) {
                completionToCall(false, applyError);
            }
            if (pendingToCall) {
                pendingToCall(false, applyError);
            }
            return;
        }
    }

    CursorValue nextCursor;
    if (extractNextCursor(response.body, nextCursor)) {
        std::string nextUrl;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdown_ || syncId != syncId_) {
                return;
            }
            const std::string baseUrl = currentPullUrl_.empty() ? pullEndpointUrl_ : currentPullUrl_;
            currentPullUrl_ = buildUrlWithCursor(baseUrl, nextCursor.value, nextCursor.encoded);
            nextUrl = currentPullUrl_;
            retryScheduled_ = false;
            retryCount_ = 0;
        }
        if (!nextUrl.empty()) {
            dispatchRequest(syncId, false);
            return;
        }
    }

    if (pushChangesCb) {
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdown_ || syncId != syncId_) {
                return;
            }

            emitLocked("{\"type\":\"phase\",\"phase\":\"push\"}");
        }

        auto self = shared_from_this();

        pushChangesCb([self, syncId](bool success, const std::string& errorMessage) {
            std::string pendingReason;
            CompletionCallback completionToCall;
            CompletionCallback pendingCompletion;
            bool shouldReturn = false;
            std::string errorCopy = errorMessage;
            {
                std::lock_guard<std::mutex> lock(self->mutex_);
                
                if (self->shutdown_ || syncId != self->syncId_) {
                    return;
                }
                
                if (!success) {
                    self->emitLocked(std::string("{\"type\":\"error\",\"message\":\"") +
                                     json_utils::escapeJsonString(errorMessage) + "\"}");
                    self->stateJson_ = "{\"state\":\"error\"}";
                    self->emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
                    self->syncInFlight_ = false;
                    self->retryScheduled_ = false;
                    self->retryCount_ = 0;
                    self->currentRequestId_.clear();
                    self->currentPullUrl_.clear();
                    completionToCall = std::move(self->completionCallback_);
                    pendingCompletion = std::move(self->pendingCompletionCallback_);
                    self->pendingCompletionCallback_ = nullptr;
                    self->pendingReason_.clear();
                    shouldReturn = true;
                }
               
                if (!shouldReturn) {
                    self->stateJson_ = "{\"state\":\"done\"}";
                    self->emitLocked("{\"type\":\"state\",\"state\":\"done\"}");
                    self->syncInFlight_ = false;
                    self->retryScheduled_ = false;
                    self->retryCount_ = 0;
                    self->currentRequestId_.clear();
                    self->currentPullUrl_.clear();
                    pendingReason = std::move(self->pendingReason_);
                    self->pendingReason_.clear();
                    completionToCall = std::move(self->completionCallback_);
                    pendingCompletion = std::move(self->pendingCompletionCallback_);
                    self->pendingCompletionCallback_ = nullptr;
                }
            }

            if (shouldReturn) {
                if (completionToCall) {
                    completionToCall(false, errorCopy);
                }
                if (pendingCompletion) {
                    pendingCompletion(false, errorCopy);
                }
                return;
            }

            if (completionToCall) {
                completionToCall(true, "");
            }

            if (!pendingReason.empty()) {
                if (pendingCompletion) {
                    self->startWithCompletion(pendingReason, std::move(pendingCompletion));
                } else {
                    self->start(pendingReason);
                }
            }
        });
        
        return;
    }

    std::string pendingReason;
    CompletionCallback completionFinal;
    CompletionCallback pendingCompletionFinal;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }
        stateJson_ = "{\"state\":\"done\"}";
        emitLocked("{\"type\":\"state\",\"state\":\"done\"}");
        syncInFlight_ = false;
        retryScheduled_ = false;
        retryCount_ = 0;
        currentRequestId_.clear();
        currentPullUrl_.clear();
        pendingReason = std::move(pendingReason_);
        pendingReason_.clear();
        completionFinal = std::move(completionCallback_);
        pendingCompletionFinal = std::move(pendingCompletionCallback_);
        pendingCompletionCallback_ = nullptr;
    }
    if (completionFinal) {
        completionFinal(true, "");
    }
    if (!pendingReason.empty()) {
        if (pendingCompletionFinal) {
            startWithCompletion(pendingReason, std::move(pendingCompletionFinal));
        } else {
            start(pendingReason);
        }
    }
}

bool SyncEngine::scheduleRetryLocked(int64_t syncId, int statusCode, const std::string& message) {
    if (shutdown_) {
        return false;
    }
    if (!shouldRetryLocked(statusCode) || retryScheduled_) {
        return false;
    }
    retryCount_++;
    int delayMs = computeBackoffMsLocked();
    retryScheduled_ = true;
    emitLocked(std::string("{\"type\":\"retry_scheduled\",\"attempt\":") + std::to_string(retryCount_ + 1) +
               ",\"delayMs\":" + std::to_string(delayMs) + ",\"message\":\"" + json_utils::escapeJsonString(message) + "\"}");
    stateJson_ = "{\"state\":\"retry_scheduled\"}";
    emitLocked("{\"type\":\"state\",\"state\":\"retry_scheduled\"}");

    auto self = shared_from_this();
    std::thread([self, syncId, delayMs]() {
        if (delayMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        }
        self->retry(syncId);
    }).detach();
    return true;
}

void SyncEngine::retry(int64_t syncId) {
    bool shouldDispatch = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_ || !syncInFlight_) {
            return;
        }
        retryScheduled_ = false;
        shouldDispatch = true;
    }
    if (shouldDispatch) {
        dispatchRequest(syncId, true);
    }
}

bool SyncEngine::shouldRetryLocked(int statusCode) const {
    if (retryCount_ >= maxRetries_) {
        return false;
    }
    if (statusCode == 0) {
        return true;
    }
    if (statusCode == 408 || statusCode == 429) {
        return true;
    }
    if (statusCode >= 500 && statusCode <= 599) {
        return true;
    }
    return false;
}

int SyncEngine::computeBackoffMsLocked() const {
    if (retryCount_ <= 0) {
        return retryInitialMs_;
    }
    int64_t delay = static_cast<int64_t>(retryInitialMs_) << (retryCount_ - 1);
    if (delay > retryMaxMs_) {
        delay = retryMaxMs_;
    }
    return static_cast<int>(delay);
}

std::string SyncEngine::getJsonStringValue(const std::string& json, const std::string& key) {
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element doc = parser.parse(json);
        std::string_view value;
        auto error = doc[key].get(value);
        if (error) {
            return "";
        }
        return std::string(value);
    } catch (...) {
        return "";
    }
}

int SyncEngine::getJsonIntValue(const std::string& json, const std::string& key, int defaultValue) {
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element doc = parser.parse(json);
        int64_t value;
        auto error = doc[key].get(value);
        if (error) {
            return defaultValue;
        }
        return static_cast<int>(value);
    } catch (...) {
        return defaultValue;
    }
}

} // namespace watermelondb
