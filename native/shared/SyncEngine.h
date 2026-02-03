#pragma once

#include "SyncPlatform.h"

#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace watermelondb {

class SyncEngine : public std::enable_shared_from_this<SyncEngine> {
public:
    using EventCallback = std::function<void(const std::string&)>;
    using ApplyCallback = std::function<bool(const std::string& payload, std::string& errorMessage)>;

    SyncEngine();

    void setEventCallback(EventCallback callback);
    void setApplyCallback(ApplyCallback callback);
    void configure(const std::string& configJson);
    void setAuthToken(const std::string& token);
    void clearAuthToken();
    void start(const std::string& reason);
    std::string stateJson() const;
    void shutdown();
    void notifyQueueDrained();

private:
    mutable std::mutex mutex_;
    EventCallback eventCallback_;
    ApplyCallback applyCallback_;
    std::string configJson_;
    std::string stateJson_ = "{\"state\":\"idle\"}";
    std::string pullEndpointUrl_;
    std::string socketioUrl_;
    std::string authToken_;
    int timeoutMs_ = 30000;
    int maxRetries_ = 3;
    int retryInitialMs_ = 1000;
    int retryMaxMs_ = 30000;
    bool syncInFlight_ = false;
    bool retryScheduled_ = false;
    int retryCount_ = 0;
    int64_t syncId_ = 0;
    std::string pendingReason_;
    std::string currentReason_;
    bool shutdown_ = false;

    void emitLocked(const std::string& eventJson);
    void dispatchRequest(int64_t syncId, bool isRetry);
    void handleHttpResponse(int64_t syncId, const platform::HttpResponse& response);
    bool scheduleRetryLocked(int64_t syncId, int statusCode, const std::string& message);
    void retry(int64_t syncId);
    bool shouldRetryLocked(int statusCode) const;
    int computeBackoffMsLocked() const;

    static std::string getJsonStringValue(const std::string& json, const std::string& key);
    static int getJsonIntValue(const std::string& json, const std::string& key, int defaultValue);
};

} // namespace watermelondb
