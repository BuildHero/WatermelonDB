#pragma once

#include <functional>

namespace watermelondb {
namespace android {
void runOnWorkQueue(const std::function<void()>& work);
bool isOnWorkQueue();
} // namespace android
} // namespace watermelondb
