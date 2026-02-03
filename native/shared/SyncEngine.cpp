#include "SyncEngine.h"
#include "JsonUtils.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <thread>

#if __has_include(<simdjson.h>)
#include <simdjson.h>
#elif __has_include("simdjson.h")
#include "simdjson.h"
#else
#error "simdjson headers not found. Please add @nozbe/simdjson or provide simdjson headers."
#endif

namespace watermelondb {

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
    retryInitialMs_ = std::max(0, getJsonIntValue(configJson_, "retryInitialMs", 1000));
    retryMaxMs_ = std::max(retryInitialMs_, getJsonIntValue(configJson_, "retryMaxMs", 30000));
    stateJson_ = "{\"state\":\"configured\"}";
    emitLocked(stateJson_);
}

void SyncEngine::setAuthToken(const std::string& token) {
    bool shouldRestart = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        authToken_ = token;
        if (!syncInFlight_ && stateJson_ == "{\"state\":\"auth_required\"}") {
            shouldRestart = true;
        }
    }
    if (shouldRestart) {
        start("auth_token_updated");
    }
}

void SyncEngine::clearAuthToken() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
        return;
    }
    authToken_.clear();
}

void SyncEngine::start(const std::string& reason) {
    bool shouldStart = false;
    int64_t syncId = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncInFlight_) {
            pendingReason_ = reason;
            emitLocked(std::string("{\"type\":\"sync_queued\",\"reason\":\"") + json_utils::escapeJsonString(reason) + "\"}");
            return;
        }
        syncInFlight_ = true;
        retryScheduled_ = false;
        retryCount_ = 0;
        currentReason_ = reason;
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
    stateJson_ = "{\"state\":\"idle\"}";
    syncId_++;
}

void SyncEngine::notifyQueueDrained() {
    std::string pendingReason;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (stateJson_ != "{\"state\":\"waiting_for_queue\"}") {
            return;
        }
        stateJson_ = "{\"state\":\"done\"}";
        emitLocked("{\"type\":\"state\",\"state\":\"done\"}");
        syncInFlight_ = false;
        retryScheduled_ = false;
        retryCount_ = 0;
        pendingReason = std::move(pendingReason_);
        pendingReason_.clear();
    }
    if (!pendingReason.empty()) {
        start(pendingReason);
    }
}

void SyncEngine::emitLocked(const std::string& eventJson) {
    if (eventCallback_) {
        eventCallback_(eventJson);
    }
}

void SyncEngine::dispatchRequest(int64_t syncId, bool isRetry) {
    std::string pullEndpointUrl;
    std::string authToken;
    int timeoutMs = 30000;
    int attempt = 1;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }
        pullEndpointUrl = pullEndpointUrl_;
        authToken = authToken_;
        timeoutMs = timeoutMs_;
        attempt = retryCount_ + 1;

        if (pullEndpointUrl.empty()) {
            emitLocked("{\"type\":\"error\",\"message\":\"Missing sync pullEndpointUrl\"}");
            stateJson_ = "{\"state\":\"error\"}";
            emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
            syncInFlight_ = false;
            retryScheduled_ = false;
            return;
        }

        stateJson_ = "{\"state\":\"syncing\"}";
        emitLocked("{\"type\":\"state\",\"state\":\"syncing\"}");
        emitLocked(std::string("{\"type\":\"phase\",\"phase\":\"pull\",\"attempt\":") + std::to_string(attempt) + "}");
        if (isRetry) {
            emitLocked(std::string("{\"type\":\"sync_retry\",\"attempt\":") + std::to_string(attempt) + "}");
        }
    }

    platform::HttpRequest request;
    request.method = "GET";
    request.url = pullEndpointUrl;
    request.timeoutMs = timeoutMs;
    if (!authToken.empty()) {
        request.headers["Authorization"] = std::string("Bearer ") + authToken;
    }

    platform::httpRequest(request, [self = shared_from_this(), syncId](const platform::HttpResponse& response) {
        self->handleHttpResponse(syncId, response);
    });
}

void SyncEngine::handleHttpResponse(int64_t syncId, const platform::HttpResponse& response) {
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
            return;
        }

        if (response.statusCode == 401 || response.statusCode == 403) {
            stateJson_ = "{\"state\":\"auth_required\"}";
            emitLocked("{\"type\":\"auth_required\"}");
            emitLocked("{\"type\":\"state\",\"state\":\"auth_required\"}");
            syncInFlight_ = false;
            retryScheduled_ = false;
            retryCount_ = 0;
            return;
        }

        if (response.statusCode >= 400) {
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
            return;
        }

        emitLocked(std::string("{\"type\":\"http\",\"phase\":\"pull\",\"status\":") +
                   std::to_string(response.statusCode) + "}");
    }

    ApplyCallback applyCb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }
        applyCb = applyCallback_;
    }

    std::string applyError;
    if (applyCb) {
        if (!applyCb(response.body, applyError)) {
            std::lock_guard<std::mutex> lock(mutex_);
            emitLocked(std::string("{\"type\":\"error\",\"message\":\"") +
                       json_utils::escapeJsonString(applyError) + "\"}");
            stateJson_ = "{\"state\":\"error\"}";
            emitLocked("{\"type\":\"state\",\"state\":\"error\"}");
            syncInFlight_ = false;
            retryScheduled_ = false;
            retryCount_ = 0;
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
            return;
        }
        if (syncId != syncId_) {
            return;
        }
        stateJson_ = "{\"state\":\"waiting_for_queue\"}";
        emitLocked("{\"type\":\"phase\",\"phase\":\"drain_queue\"}");
        emitLocked("{\"type\":\"drain_queue\"}");
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
