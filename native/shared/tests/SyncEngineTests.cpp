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

    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
        watermelondb::platform::HttpResponse response;
        response.statusCode = 200;
        response.body = "{}";
        done(response);
    });

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1}");
    engine->start("test");

    expectTrue(recorder.waitForContains("\"type\":\"drain_queue\""), "expected drain_queue event");
    engine->notifyQueueDrained();
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1}");
    engine->start("auth");

    expectTrue(recorder.waitForContains("\"type\":\"auth_required\""), "expected auth_required event");
}

void test_retry_flow() {
    EventRecorder recorder;
    auto engine = std::make_shared<watermelondb::SyncEngine>();
    engine->setEventCallback([&](const std::string& eventJson) { recorder.add(eventJson); });
    engine->setApplyCallback([&](const std::string&, std::string&) { return true; });

    static int callCount = 0;
    callCount = 0;
    watermelondb::platform::setHttpHandler([](const watermelondb::platform::HttpRequest&,
                                              std::function<void(const watermelondb::platform::HttpResponse&)> done) {
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1,"
                      "\"maxRetries\":1,\"retryInitialMs\":0,\"retryMaxMs\":0}");
    engine->start("retry");

    expectTrue(recorder.waitForContains("\"type\":\"retry_scheduled\""), "expected retry_scheduled event");
    expectTrue(recorder.waitForContains("\"type\":\"drain_queue\""), "expected drain_queue after retry");
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1}");
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1}");
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

void test_queue_when_in_flight() {
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1}");
    engine->start("first");
    engine->start("second");

    expectTrue(recorder.waitForContains("\"type\":\"sync_queued\""), "expected sync_queued event");
    engine->notifyQueueDrained();
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1,"
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

    expectTrue(recorder.waitForContains("\"message\":\"Missing sync endpoint\""),
               "expected missing endpoint error");
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

    expectTrue(recorder.waitForContains("\"message\":\"Missing sync endpoint\""),
               "expected missing endpoint error from invalid config");
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

    engine->configure("{\"endpoint\":\"https://example.com\",\"connectionTag\":1}");
    engine->start("apply_error");

    expectTrue(recorder.waitForContains("\"message\":\"apply failed\""), "expected apply error message");
    expectTrue(recorder.waitForContains("\"state\":\"error\""), "expected error state after apply failure");
}

} // namespace

int main() {
    test_success_flow();
    test_auth_required();
    test_retry_flow();
    test_shutdown_prevents_events();
    test_auth_token_restart();
    test_queue_when_in_flight();
    test_backoff_delay_cap();
    test_missing_endpoint_error();
    test_invalid_config_json();
    test_apply_error_sets_state();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SyncEngine tests passed\n";
    return 0;
}
