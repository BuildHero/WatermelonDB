#pragma once

#include <functional>
#include <string>
#include <cstdint>
#include <memory>

namespace watermelondb {
namespace platform {

// Memory alert levels
enum class MemoryAlertLevel {
    WARN,      // Memory pressure warning
    CRITICAL   // Critical memory pressure
};

// Platform-specific initialization
void initializeWorkQueue();

// Calculate optimal batch size based on device hardware
unsigned long calculateOptimalBatchSize();

class MemoryAlertHandle {
public:
    virtual ~MemoryAlertHandle() = default;
    virtual void cancel() = 0;
};

// Setup memory pressure monitoring
// Callback receives MemoryAlertLevel when system memory is low
std::shared_ptr<MemoryAlertHandle> setupMemoryAlertCallback(const std::function<void(MemoryAlertLevel)>& callback);

// Cancel all memory pressure monitoring callbacks
void cancelMemoryPressureMonitoring();

// Download file from URL
// onData: called with each chunk of data received
// onComplete: called with error message (empty if success)
class DownloadHandle {
public:
    virtual ~DownloadHandle() = default;
    virtual void cancel() = 0;
};

std::shared_ptr<DownloadHandle> downloadFile(
    const std::string& url,
    std::function<void(const uint8_t* data, size_t length)> onData,
    std::function<void(const std::string& errorMessage)> onComplete
);

// Log functions
void logInfo(const std::string& message);
void logDebug(const std::string& message);
void logError(const std::string& message);

} // namespace platform
} // namespace watermelondb
