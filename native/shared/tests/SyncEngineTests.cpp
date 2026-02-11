#include "../SyncEngine.h"
#include "../SyncPlatform.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

namespace watermelondb::platform {

using HttpHandler = std::function<void(const HttpRequest&, std::function<void(const HttpResponse&)>)>;

static std::mutex gHttpMutex;
static HttpHandler gHttpHandler;

void setHttpHandler(HttpHandler handler) {
    std::lock_guard<std::mutex> lock(gHttpMutex);
    gHttpHandler = std::move(handler);
}

void httpRequest(const HttpRequest& request, std::function<void(const HttpResponse&)> onComplete) {
    HttpHandler handler;
    {
        std::lock_guard<std::mutex> lock(gHttpMutex);
        handler = gHttpHandler;
    }
    if (handler) {
        handler(request, std::move(onComplete));
        return;
    }
    HttpResponse response;
    response.statusCode = 500;
    response.errorMessage = "No http handler configured";
    onComplete(response);
}

std::string generateRequestId() {
    static int counter = 0;
    counter++;
    return "test-request-id-" + std::to_string(counter);
}

} // namespace watermelondb::platform

namespace {

struct EventRecorder {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::string> events;

    void add(const std::string& eventJson) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            events.push_back(eventJson);
        }
        cv.notify_all();
    }

    bool waitForContains(const std::string& needle, int timeoutMs = 500) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [&]() {
            for (const auto& event : events) {
                if (event.find(needle) != std::string::npos) {
                    return true;
                }
            }
            return false;
        });
    }
};

static int gFailures = 0;

void expectTrue(bool value, const char* message) {
    if (!value) {
        std::cerr << "FAIL: " << message << "\n";
        gFailures++;
    }
}

void test_success_flow() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> completion) {
        completion(true, "");
    });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->start("test");

    expectTrue(recorder.waitForContains("\"state\":\"done\""), "expected done state");
}

void test_auth_required() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 401;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->start("auth");

    expectTrue(recorder.waitForContains("\"type\":\"auth_required\""), "expected auth_required event");
}

void test_retry_flow() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> completion) {
        completion(true, "");
    });

    static int callCount = 0;
    static std::string requestId;
    callCount = 0;
    requestId.clear();
    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest& request,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        auto headerIt = request.headers.find("X-Request-Id");
        expectTrue(headerIt != request.headers.end(), "expected X-Request-Id header");
        if (headerIt != request.headers.end()) {
            expectTrue(!headerIt->second.empty(), "expected X-Request-Id to be non-empty");
            if (callCount == 0) {
                requestId = headerIt->second;
            } else {
                expectTrue(headerIt->second == requestId, "expected X-Request-Id stable across retries");
            }
        }
        watermelondb::platform::HttpResponse response;
        if (callCount == 0) {
            response.statusCode = 500;
        } else {
            response.statusCode = 200;
            response.body = "{}";
        }
        callCount++;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1,"
                      "\"maxRetries\":1,\"retryInitialMs\":0,\"retryMaxMs\":0}");
    engine->start("retry");

    expectTrue(recorder.waitForContains("\"type\":\"retry_scheduled\""), "expected retry_scheduled event");
    expectTrue(recorder.waitForContains("\"state\":\"done\""), "expected done after retry");
}

void test_cursor_pagination() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> completion) {
        completion(true, "");
    });

    static int callCount = 0;
    callCount = 0;
    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest& request,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        if (callCount == 0) {
            response.statusCode = 200;
            response.body = "{\"changes\":{},\"next\":{\"foo\":\"bar\"}}";
            expectTrue(request.url.find("sequenceId=seq-1") != std::string::npos,
                       "expected sequenceId param on first page");
        } else {
            response.statusCode = 200;
            response.body = "{\"changes\":{},\"next\":null}";
            expectTrue(request.url.find("cursor=%7B%22foo%22%3A%22bar%22%7D") != std::string::npos,
                       "expected cursor param on next page");
        }
        callCount++;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull?sequenceId=seq-1\",\"connectionTag\":1}");
    engine->start("pagination");

    expectTrue(recorder.waitForContains("\"state\":\"done\""), "expected done after pagination");
}

void test_auth_required_resumes_cursor() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    engine->setAuthTokenRequestCallback([engine]() { engine->setAuthToken("token-2"); });
    engine->setAuthToken("token-1");

    static int callCount = 0;
    callCount = 0;
    std::string requestId;
    watermelondb::platform::setHttpHandler([&](const watermelondb::platform::HttpRequest& request,
                                               std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        auto headerIt = request.headers.find("X-Request-Id");
        expectTrue(headerIt != request.headers.end(), "expected X-Request-Id header");
        if (headerIt != request.headers.end()) {
            if (requestId.empty()) {
                requestId = headerIt->second;
            } else {
                expectTrue(headerIt->second == requestId, "expected X-Request-Id stable across auth retry");
            }
        }

        watermelondb::platform::HttpResponse response;
        if (callCount == 0) {
            response.statusCode = 200;
            response.body = "{\"changes\":{},\"next\":\"cursor-token\"}";
            expectTrue(request.url.find("sequenceId=seq-1") != std::string::npos,
                       "expected sequenceId param on first page");
        } else if (callCount == 1) {
            response.statusCode = 401;
        } else {
            response.statusCode = 200;
            response.body = "{\"changes\":{},\"next\":null}";
            expectTrue(request.url.find("cursor=cursor-token") != std::string::npos,
                       "expected cursor param after auth refresh");
            auto authIt = request.headers.find("Authorization");
            expectTrue(authIt != request.headers.end() && authIt->second == "token-2",
                       "expected refreshed auth token");
        }
        callCount++;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull?sequenceId=seq-1\",\"connectionTag\":1}");
    engine->start("auth_pagination");

    expectTrue(recorder.waitForContains("\"state\":\"done\""), "expected done after auth refresh");
}

void test_shutdown_prevents_events() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->shutdown();
    engine->start("after_shutdown");

    expectTrue(!recorder.waitForContains("\"type\":\"sync_start\""), "no events after shutdown");
}

void test_auth_token_restart() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 401;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->start("auth");
    expectTrue(recorder.waitForContains("\"type\":\"auth_required\""), "expected auth_required event");

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    engine->setAuthToken("token");
    expectTrue(recorder.waitForContains("\"type\":\"sync_start\""), "expected restart after auth token");
}

void test_completion_preserved_after_auth_refresh() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    std::mutex completionMutex;
    std::condition_variable completionCv;
    bool completed = false;
    bool completedSuccess = false;

    static int callCount = 0;
    callCount = 0;
    watermelondb::platform::setHttpHandler([&](const watermelondb::platform::HttpRequest&,
                                               std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        if (callCount == 0) {
            response.statusCode = 401;
        } else {
            response.statusCode = 200;
            response.body = "{}";
        }
        callCount++;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->setAuthToken("expired-token");
    engine->startWithCompletion("auth_refresh_completion", [&](bool success, const std::string&) {
        {
            std::lock_guard<std::mutex> lock(completionMutex);
            completed = true;
            completedSuccess = success;
        }
        completionCv.notify_all();
    });

    expectTrue(recorder.waitForContains("\"type\":\"auth_required\""), "expected auth_required event");
    engine->setAuthToken("new-token");

    std::unique_lock<std::mutex> lock(completionMutex);
    bool finished = completionCv.wait_for(lock, std::chrono::milliseconds(500), [&]() {
        return completed;
    });
    expectTrue(finished, "expected completion callback to be called after auth refresh");
    expectTrue(completedSuccess, "expected completion to be successful after auth refresh");
}

void test_queue_when_in_flight() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });
    std::function<void(bool, const std::string&)> pushCompletion;
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> completion) {
        pushCompletion = std::move(completion);
    });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->start("first");
    engine->start("second");

    expectTrue(recorder.waitForContains("\"type\":\"sync_queued\""), "expected sync_queued event");
    if (pushCompletion) {
        pushCompletion(true, "");
    }
    expectTrue(recorder.waitForContains("\"reason\":\"second\""), "expected second reason to run");
}

void test_backoff_delay_cap() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 500;
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1,"
                      "\"maxRetries\":3,\"retryInitialMs\":10,\"retryMaxMs\":15}");
    engine->start("retry");

    expectTrue(recorder.waitForContains("\"type\":\"retry_scheduled\""), "expected retry_scheduled event");
    expectTrue(recorder.waitForContains("\"delayMs\":15"), "expected delayMs capped to retryMaxMs");
}

void test_missing_endpoint_error() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    watermelondb::platform::setHttpHandler(nullptr);

    engine->configure("{\"connectionTag\":1}");
    engine->start("missing");

    expectTrue(recorder.waitForContains("\"message\":\"Missing sync pullEndpointUrl\""),
               "expected missing pullEndpointUrl error");
    expectTrue(recorder.waitForContains("\"state\":\"error\""), "expected error state");
}

void test_invalid_config_json() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    watermelondb::platform::setHttpHandler(nullptr);

    engine->configure("{invalid");
    engine->start("invalid_config");

    expectTrue(recorder.waitForContains("\"message\":\"Missing sync pullEndpointUrl\""),
               "expected missing pullEndpointUrl error from invalid config");
    expectTrue(recorder.waitForContains("\"state\":\"error\""), "expected error state");
}

void test_apply_error_sets_state() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string& errorMessage) {
        errorMessage = "apply failed";
        return false;
    });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->start("apply_error");

    expectTrue(recorder.waitForContains("\"message\":\"apply failed\""), "expected apply error message");
    expectTrue(recorder.waitForContains("\"state\":\"error\""), "expected error state after apply failure");
}

void test_cancel_sync_when_idle() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->cancelSync();

    expectTrue(!recorder.waitForContains("sync_cancelled", 100), "no sync_cancelled when idle");
    std::string state = engine->stateJson();
    expectTrue(state.find("\"state\":\"configured\"") != std::string::npos,
               "state should remain configured after idle cancel");
}

void test_cancel_sync_in_flight() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    // Hold sync in push phase so we can cancel it
    std::function<void(bool, const std::string&)> pushCompletion;
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> cb) {
        pushCompletion = std::move(cb);
        // Don't call cb — sync stays in push phase
    });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    std::mutex m;
    std::condition_variable cv;
    bool completed = false;
    std::string completionError;

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->setAuthToken("token");
    engine->startWithCompletion("test", [&](bool, const std::string& error) {
        {
            std::lock_guard<std::mutex> lock(m);
            completed = true;
            completionError = error;
        }
        cv.notify_all();
    });

    expectTrue(recorder.waitForContains("\"phase\":\"push\""), "expected push phase");
    engine->cancelSync();

    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return completed; });
    }

    expectTrue(completed, "completion should fire on cancel");
    expectTrue(completionError == "cancelled_for_foreground", "error should be cancelled_for_foreground");
    expectTrue(recorder.waitForContains("sync_cancelled"), "expected sync_cancelled event");
    expectTrue(engine->stateJson().find("\"state\":\"idle\"") != std::string::npos,
               "state should be idle after cancel");
}

void test_cancel_sync_during_auth_required() {
    // This tests the critical bug fix: cancelSync must handle auth_required state
    // where syncInFlight_=false but completionCallback_ is still set.
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });
    engine->setAuthTokenRequestCallback([]() {
        // Don't provide a token — simulates JS auth provider not responding yet
    });

    // No auth token set — dispatchRequest will enter auth_required immediately
    watermelondb::platform::setHttpHandler(nullptr);

    std::mutex m;
    std::condition_variable cv;
    bool completed = false;
    std::string completionError;

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->startWithCompletion("bg_sync", [&](bool, const std::string& error) {
        {
            std::lock_guard<std::mutex> lock(m);
            completed = true;
            completionError = error;
        }
        cv.notify_all();
    });

    expectTrue(recorder.waitForContains("\"type\":\"auth_required\""), "expected auth_required event");

    // Cancel while in auth_required — must fire completion even though syncInFlight_=false
    engine->cancelSync();

    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return completed; });
    }

    expectTrue(completed, "completion should fire during auth_required cancel");
    expectTrue(completionError == "cancelled_for_foreground",
               "error should be cancelled_for_foreground during auth cancel");
    expectTrue(engine->stateJson().find("\"state\":\"idle\"") != std::string::npos,
               "state should be idle after auth cancel");

    // Verify foreground sync can proceed after cancelling auth_required
    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });
    engine->setPushChangesCallback([](std::function<void(bool, const std::string&)> cb) { cb(true, ""); });
    engine->setAuthToken("new-token");
    engine->start("foreground");

    expectTrue(recorder.waitForContains("\"reason\":\"foreground\""), "foreground sync should start");
    expectTrue(recorder.waitForContains("\"state\":\"done\""), "foreground sync should complete");
}

void test_cancel_sync_fires_pending_completion() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    // Hold sync in push phase
    std::function<void(bool, const std::string&)> pushCompletion;
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> cb) {
        pushCompletion = std::move(cb);
    });

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    std::mutex m;
    std::condition_variable cv;
    bool firstCompleted = false;
    bool secondCompleted = false;

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->setAuthToken("token");

    engine->startWithCompletion("first", [&](bool, const std::string&) {
        std::lock_guard<std::mutex> lock(m);
        firstCompleted = true;
        cv.notify_all();
    });
    expectTrue(recorder.waitForContains("\"phase\":\"push\""), "expected push phase");

    engine->startWithCompletion("second", [&](bool, const std::string&) {
        std::lock_guard<std::mutex> lock(m);
        secondCompleted = true;
        cv.notify_all();
    });
    expectTrue(recorder.waitForContains("\"type\":\"sync_queued\""), "expected sync_queued");

    engine->cancelSync();

    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return firstCompleted && secondCompleted; });
    }

    expectTrue(firstCompleted, "first completion should fire on cancel");
    expectTrue(secondCompleted, "pending completion should fire on cancel");
}

void test_get_push_changes_callback() {
    auto engine = std::make_shared<watermelondb::SyncEngine>();

    auto cb = engine->getPushChangesCallback();
    expectTrue(!cb, "push callback should be null initially");

    bool called = false;
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> completion) {
        called = true;
        completion(true, "");
    });

    auto retrieved = engine->getPushChangesCallback();
    expectTrue(!!retrieved, "push callback should be non-null after set");

    retrieved([](bool, const std::string&) {});
    expectTrue(called, "retrieved callback should invoke the original");
}

void test_cancel_restores_push_callback_via_completion() {
    // Simulates the BackgroundSyncBridge pattern: save push, set no-op, start, cancel
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    bool realPushCalled = false;
    engine->setPushChangesCallback([&](std::function<void(bool, const std::string&)> completion) {
        realPushCalled = true;
        completion(true, "");
    });

    // Hold HTTP so sync stays in flight
    std::mutex httpMutex;
    std::condition_variable httpCv;
    bool httpReceived = false;
    watermelondb::platform::setHttpHandler([&](const watermelondb::platform::HttpRequest&,
                                               std::function<void(const watermelondb::platform::HttpResponse&)>) {
        std::lock_guard<std::mutex> lock(httpMutex);
        httpReceived = true;
        httpCv.notify_all();
        // Don't call done — keeps sync in HTTP phase
    });

    engine->configure("{\"pullEndpointUrl\":\"https://example.com/pull\",\"connectionTag\":1}");
    engine->setAuthToken("token");

    // Background sync pattern: save real push, set no-op, start
    auto savedPush = engine->getPushChangesCallback();
    engine->setPushChangesCallback([](std::function<void(bool, const std::string&)> cb) {
        if (cb) cb(true, "");
    });

    engine->startWithCompletion("background_task",
        [engine, savedPush](bool, const std::string&) {
            if (savedPush) {
                engine->setPushChangesCallback(savedPush);
            }
        });

    {
        std::unique_lock<std::mutex> lock(httpMutex);
        httpCv.wait_for(lock, std::chrono::milliseconds(500), [&] { return httpReceived; });
    }

    engine->cancelSync();

    // Push callback should be restored by the completion handler
    auto currentPush = engine->getPushChangesCallback();
    expectTrue(!!currentPush, "push callback should be restored after cancel");

    currentPush([](bool, const std::string&) {});
    expectTrue(realPushCalled, "restored callback should be the original push callback");
}

} // namespace

int main() {
    test_success_flow();
    test_auth_required();
    test_retry_flow();
    test_cursor_pagination();
    test_auth_required_resumes_cursor();
    test_shutdown_prevents_events();
    test_auth_token_restart();
    test_completion_preserved_after_auth_refresh();
    test_queue_when_in_flight();
    test_backoff_delay_cap();
    test_missing_endpoint_error();
    test_invalid_config_json();
    test_apply_error_sets_state();
    test_cancel_sync_when_idle();
    test_cancel_sync_in_flight();
    test_cancel_sync_during_auth_required();
    test_cancel_sync_fires_pending_completion();
    test_get_push_changes_callback();
    test_cancel_restores_push_callback_via_completion();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SyncEngine tests passed\n";
    return 0;
}
