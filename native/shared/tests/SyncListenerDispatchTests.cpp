#include "../SyncListenerDispatch.h"

#include <hermes/hermes.h>
#include <jsi/jsi.h>

#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace facebook;

namespace {

static int gFailures = 0;

void expectTrue(bool value, const char* message) {
    if (!value) {
        std::cerr << "FAIL: " << message << "\n";
        gFailures++;
    }
}

struct TestSyncEventState {
    std::mutex mutex;
    std::unordered_map<int64_t, std::shared_ptr<jsi::Function>> listeners;
    jsi::Runtime* runtime = nullptr;
    bool alive = true;
};

// Mirrors the platform modules' removeSyncListener: it takes the same non-recursive mutex.
void removeListener(TestSyncEventState& state, int64_t id) {
    const std::lock_guard<std::mutex> lock(state.mutex);
    state.listeners.erase(id);
}

void addListener(TestSyncEventState& state, jsi::Runtime& rt, int64_t id, std::function<void()> body) {
    auto fn = jsi::Function::createFromHostFunction(
        rt,
        jsi::PropNameID::forAscii(rt, "listener"),
        1,
        [body](jsi::Runtime&, const jsi::Value&, const jsi::Value*, size_t) -> jsi::Value {
            body();
            return jsi::Value::undefined();
        });
    const std::lock_guard<std::mutex> lock(state.mutex);
    state.listeners.emplace(id, std::make_shared<jsi::Function>(std::move(fn)));
}

// A deadlock never returns, so run the dispatch on a worker and bail out of the whole
// process if it doesn't finish (a stuck thread can't be joined).
void dispatchOrDie(TestSyncEventState& state, const std::string& eventJson, const char* testName) {
    auto task = std::async(std::launch::async, [&state, &eventJson]() {
        watermelondb::dispatchSyncEvent(state, eventJson);
    });
    if (task.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        std::cerr << "FAIL: " << testName << " deadlocked in dispatchSyncEvent\n";
        std::_Exit(1);
    }
    task.get();
}

void test_listenerCanUnsubscribeItselfDuringDispatch() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    TestSyncEventState state;
    state.runtime = runtime.get();
    int calls = 0;

    addListener(state, *runtime, 1, [&]() {
        calls++;
        removeListener(state, 1);
    });

    dispatchOrDie(state, "{\"type\":\"sync_cancelled\"}", "self-unsubscribe");
    expectTrue(calls == 1, "self-unsubscribing listener runs once");
    expectTrue(state.listeners.empty(), "self-unsubscribing listener is removed");

    dispatchOrDie(state, "{\"type\":\"sync_cancelled\"}", "self-unsubscribe second event");
    expectTrue(calls == 1, "removed listener is not called again");
}

void test_listenerRemovedMidDispatchIsSkipped() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    TestSyncEventState state;
    state.runtime = runtime.get();
    int firstCalls = 0;
    int secondCalls = 0;

    // unordered_map order is unspecified, so each listener removes the other; whichever runs
    // first must prevent the other from running.
    addListener(state, *runtime, 1, [&]() {
        firstCalls++;
        removeListener(state, 2);
    });
    addListener(state, *runtime, 2, [&]() {
        secondCalls++;
        removeListener(state, 1);
    });

    dispatchOrDie(state, "{\"status\":\"cdc\"}", "remove-other");
    expectTrue(firstCalls + secondCalls == 1, "a listener removed mid-dispatch is skipped");
}

void test_listenerAddedMidDispatchWaitsForNextEvent() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    TestSyncEventState state;
    state.runtime = runtime.get();
    int lateCalls = 0;
    bool added = false;

    addListener(state, *runtime, 1, [&]() {
        if (!added) {
            added = true;
            addListener(state, *runtime, 2, [&]() { lateCalls++; });
        }
    });

    dispatchOrDie(state, "{\"status\":\"cdc\"}", "add-during-dispatch");
    expectTrue(lateCalls == 0, "listener added mid-dispatch doesn't see the in-flight event");

    dispatchOrDie(state, "{\"status\":\"cdc\"}", "add-during-dispatch second event");
    expectTrue(lateCalls == 1, "listener added mid-dispatch sees the next event");
}

void test_deadStateSkipsListeners() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    TestSyncEventState state;
    state.runtime = runtime.get();
    int calls = 0;

    addListener(state, *runtime, 1, [&]() { calls++; });
    state.alive = false;

    dispatchOrDie(state, "{\"status\":\"cdc\"}", "dead-state");
    expectTrue(calls == 0, "no listener runs once the module is torn down");
}

void test_listenerReceivesEventJson() {
    auto runtime = facebook::hermes::makeHermesRuntime();
    auto& rt = *runtime;
    TestSyncEventState state;
    state.runtime = runtime.get();
    std::string received;

    auto fn = jsi::Function::createFromHostFunction(
        rt,
        jsi::PropNameID::forAscii(rt, "listener"),
        1,
        [&received](jsi::Runtime& rt2, const jsi::Value&, const jsi::Value* args, size_t count) -> jsi::Value {
            if (count > 0 && args[0].isString()) {
                received = args[0].asString(rt2).utf8(rt2);
            }
            return jsi::Value::undefined();
        });
    state.listeners.emplace(1, std::make_shared<jsi::Function>(std::move(fn)));

    dispatchOrDie(state, "{\"state\":\"done\"}", "event-json");
    expectTrue(received == "{\"state\":\"done\"}", "listener receives the event JSON string");
}

} // namespace

int main() {
    test_listenerCanUnsubscribeItselfDuringDispatch();
    test_listenerRemovedMidDispatchIsSkipped();
    test_listenerAddedMidDispatchWaitsForNextEvent();
    test_deadStateSkipsListeners();
    test_listenerReceivesEventJson();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "sync_listener_dispatch_tests: all passed\n";
    return 0;
}
