#pragma once

#include <jsi/jsi.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace watermelondb {

// Calls every sync listener on the JS thread WITHOUT holding `state.mutex`. A listener that
// re-enters the module (removeSyncListener, getSyncStateJson, setAuthToken, ...) takes that same
// non-recursive mutex, so invoking it under the lock self-deadlocks the JS thread (MOBILE-7745).
// `State` needs: std::mutex mutex; map<int64_t, std::shared_ptr<jsi::Function>> listeners;
// jsi::Runtime* runtime; bool alive.
template <typename State>
void dispatchSyncEvent(State &state, const std::string &eventJson) {
    facebook::jsi::Runtime *runtime = nullptr;
    std::vector<std::pair<int64_t, std::shared_ptr<facebook::jsi::Function>>> snapshot;
    {
        const std::lock_guard<std::mutex> lock(state.mutex);
        if (!state.alive || !state.runtime || state.listeners.empty()) {
            return;
        }
        runtime = state.runtime;
        snapshot.assign(state.listeners.begin(), state.listeners.end());
    }

    for (auto &entry : snapshot) {
        {
            // Skip listeners removed by an earlier listener during this same dispatch.
            const std::lock_guard<std::mutex> lock(state.mutex);
            if (!state.alive) {
                return;
            }
            if (state.listeners.find(entry.first) == state.listeners.end()) {
                continue;
            }
        }
        facebook::jsi::Runtime &rt = *runtime;
        entry.second->call(rt, facebook::jsi::String::createFromUtf8(rt, eventJson));
    }
}

} // namespace watermelondb
