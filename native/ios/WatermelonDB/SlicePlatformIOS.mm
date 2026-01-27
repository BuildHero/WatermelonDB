#include "SlicePlatform.h"

#import <Foundation/Foundation.h>
#import <os/log.h>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

struct SliceDownloadCallbackState {
    std::function<void(const uint8_t* data, size_t length)> onData;
    std::function<void(const std::string& errorMessage)> onComplete;
    std::atomic<bool> completed{false};
    std::atomic<bool> cancelled{false};
    dispatch_queue_t callbackQueue = nullptr;
    dispatch_group_t callbackGroup = nullptr;
    std::vector<uint8_t> buffer;
};

@interface SliceDownloadDelegate : NSObject <NSURLSessionDataDelegate>
@property (nonatomic, weak) NSURLSession *session;
- (instancetype)initWithState:(std::shared_ptr<SliceDownloadCallbackState>)state;
@end

@implementation SliceDownloadDelegate {
    std::shared_ptr<SliceDownloadCallbackState> _state;
}

- (instancetype)initWithState:(std::shared_ptr<SliceDownloadCallbackState>)state {
    self = [super init];
    if (self) {
        _state = state;
    }
    return self;
}

- (void)URLSession:(NSURLSession *)session
          dataTask:(NSURLSessionDataTask *)dataTask
    didReceiveData:(NSData *)data {
    if (!_state || _state->completed.load()) {
        return;
    }
    if (_state->cancelled.load()) {
        return;
    }
    
    dispatch_group_t group = _state->callbackGroup;
    dispatch_group_enter(group);
    dispatch_async(_state->callbackQueue, ^{
        auto state = self->_state;
        if (!state) {
            dispatch_group_leave(group);
            return;
        }
        if (!state->cancelled.load()) {
            constexpr size_t STREAM_BUFFER_TARGET = 256 * 1024;
            const uint8_t *bytes = (const uint8_t *)data.bytes;
            size_t length = (size_t)data.length;
            if (length > 0) {
                state->buffer.insert(state->buffer.end(), bytes, bytes + length);
            }
            if (state->buffer.size() >= STREAM_BUFFER_TARGET) {
                state->onData(state->buffer.data(), state->buffer.size());
                state->buffer.clear();
            }
        }
        dispatch_group_leave(state->callbackGroup);
    });
}

- (void)URLSession:(NSURLSession *)session
              task:(NSURLSessionTask *)task
didCompleteWithError:(NSError *)error {
    if (!_state || _state->completed.load()) {
        return;
    }
    
    dispatch_group_notify(_state->callbackGroup, _state->callbackQueue, ^{
        auto state = self->_state;
        if (!state) {
            return;
        }
        if (!state->cancelled.load() && !state->buffer.empty()) {
            state->onData(state->buffer.data(), state->buffer.size());
            state->buffer.clear();
        }
        
        if (error) {
            std::string errorMsg = [[error localizedDescription] UTF8String];
            state->onComplete(errorMsg);
        } else {
            state->onComplete("");
        }
        state->completed.store(true);
    });
    
    if (self.session) {
        [self.session finishTasksAndInvalidate];
    }
}

@end

namespace watermelondb {
namespace platform {

static os_log_t logger = os_log_create("com.buildops.watermelon.slice", "SlicePlatform");

static dispatch_queue_t workQueue = dispatch_queue_create("com.buildops.watermelon.slice.importer", DISPATCH_QUEUE_SERIAL);

dispatch_source_t memoryPressureSource = nullptr;

static void *kSliceImporterQueueKey = &kSliceImporterQueueKey;

static std::mutex memoryCallbackMutex;
static std::unordered_map<int, std::function<void(MemoryAlertLevel)>> memoryCallbacks;
static int nextMemoryCallbackId = 1;

class DownloadTaskIOS : public DownloadHandle {
public:
    DownloadTaskIOS(NSURLSessionDataTask *task,
                    NSURLSession *session,
                    std::shared_ptr<SliceDownloadCallbackState> state)
        : task_(task)
        , session_(session)
        , state_(std::move(state)) {}
    
    ~DownloadTaskIOS() override {
        if (session_) {
            [session_ invalidateAndCancel];
        }
    }
    
    void cancel() override {
        if (task_) {
            [task_ cancel];
        }
        if (state_) {
            state_->cancelled.store(true);
        }
    }
    
private:
    __strong NSURLSessionDataTask *task_;
    __strong NSURLSession *session_;
    std::shared_ptr<SliceDownloadCallbackState> state_;
};

class MemoryAlertHandleIOS : public MemoryAlertHandle {
public:
    explicit MemoryAlertHandleIOS(int callbackId) : callbackId_(callbackId) {}
    
    void cancel() override {
        std::lock_guard<std::mutex> lock(memoryCallbackMutex);
        auto it = memoryCallbacks.find(callbackId_);
        if (it != memoryCallbacks.end()) {
            memoryCallbacks.erase(it);
        }
        if (memoryCallbacks.empty() && memoryPressureSource) {
            dispatch_source_cancel(memoryPressureSource);
            memoryPressureSource = nullptr;
        }
    }
    
private:
    int callbackId_;
};

void initializeWorkQueue() {
    dispatch_queue_set_specific(workQueue, kSliceImporterQueueKey, kSliceImporterQueueKey, NULL);
}

unsigned long calculateOptimalBatchSize() {
    // Get device memory
    NSProcessInfo *processInfo = [NSProcessInfo processInfo];
    
    unsigned long long physicalMemory = processInfo.physicalMemory;
    
    // Get available processors for concurrency considerations
    NSInteger processorCount = processInfo.activeProcessorCount;
    
    // Calculate batch size based on memory tiers
    unsigned long batchSize;
    
    if (physicalMemory >= 6ULL * 1024 * 1024 * 1024) {
        // 6+ GB RAM (iPhone 12 Pro and newer, iPad Pro)
        // Can handle large batches, but cap at 2000 to be safe
        batchSize = 2000L;
    } else if (physicalMemory >= 4ULL * 1024 * 1024 * 1024) {
        // 4-6 GB RAM (iPhone 11 Pro, iPhone 12, iPad Air)
        batchSize = 1500L;
    } else if (physicalMemory >= 3ULL * 1024 * 1024 * 1024) {
        // 3-4 GB RAM (iPhone X, iPhone 11, older iPads)
        batchSize = 1000L;
    } else if (physicalMemory >= 2ULL * 1024 * 1024 * 1024) {
        // 2-3 GB RAM (iPhone 8, iPhone SE 2)
        batchSize = 500L;
    } else {
        // < 2 GB RAM (very old devices)
        batchSize = 250L;
    }
    
    // Adjust for low processor count (older/slower devices)
    if (processorCount <= 2) {
        batchSize = batchSize / 2;
    }

    return batchSize;
}

std::shared_ptr<MemoryAlertHandle> setupMemoryAlertCallback(const std::function<void(MemoryAlertLevel)>& callback) {
    std::lock_guard<std::mutex> lock(memoryCallbackMutex);
    
    if (!memoryPressureSource) {
        // Monitor system memory pressure and adapt batch size accordingly
        dispatch_source_t source = dispatch_source_create(
                                                          DISPATCH_SOURCE_TYPE_MEMORYPRESSURE,
                                                          0,
                                                          DISPATCH_MEMORYPRESSURE_WARN | DISPATCH_MEMORYPRESSURE_CRITICAL,
                                                          workQueue
                                                          );
        
        if (!source) {
            os_log_error(logger, "Failed to create memory pressure source");
            return nullptr;
        }
        
        dispatch_source_set_event_handler(source, ^{
            std::vector<std::function<void(MemoryAlertLevel)>> callbacksCopy;
            {
                std::lock_guard<std::mutex> innerLock(memoryCallbackMutex);
                callbacksCopy.reserve(memoryCallbacks.size());
                for (const auto &entry : memoryCallbacks) {
                    callbacksCopy.push_back(entry.second);
                }
            }
            
            unsigned long flags = dispatch_source_get_data(source);
            if (flags & DISPATCH_MEMORYPRESSURE_CRITICAL) {
                for (const auto &cb : callbacksCopy) {
                    cb(MemoryAlertLevel::CRITICAL);
                }
                os_log_error(logger, "CRITICAL memory pressure!");
            } else if (flags & DISPATCH_MEMORYPRESSURE_WARN) {
                for (const auto &cb : callbacksCopy) {
                    cb(MemoryAlertLevel::WARN);
                }
                os_log_error(logger, "Memory pressure warning.");
            }
        });
        
        dispatch_source_set_cancel_handler(source, ^{
            // Cleanup
        });
        
        memoryPressureSource = source;
        dispatch_resume(source);
    }
    
    int callbackId = nextMemoryCallbackId++;
    memoryCallbacks[callbackId] = callback;
    
    return std::make_shared<MemoryAlertHandleIOS>(callbackId);
}

void cancelMemoryPressureMonitoring() {
    std::lock_guard<std::mutex> lock(memoryCallbackMutex);
    memoryCallbacks.clear();
    if (memoryPressureSource) {
        dispatch_source_cancel(memoryPressureSource);
        memoryPressureSource = nullptr;
    }
}

std::shared_ptr<DownloadHandle> downloadFile(
    const std::string& url,
    std::function<void(const uint8_t* data, size_t length)> onData,
    std::function<void(const std::string& errorMessage)> onComplete
) {
    // Create URL
    NSURL *nsURL = [NSURL URLWithString:[NSString stringWithUTF8String:url.c_str()]];
    if (!nsURL) {
        onComplete("Invalid URL");
        return nullptr;
    }
    
    auto state = std::make_shared<SliceDownloadCallbackState>();
    state->onData = std::move(onData);
    state->onComplete = std::move(onComplete);
    state->callbackQueue = workQueue;
    state->callbackGroup = dispatch_group_create();
    
    NSURLSessionConfiguration *config = [NSURLSessionConfiguration defaultSessionConfiguration];
    SliceDownloadDelegate *delegate = [[SliceDownloadDelegate alloc] initWithState:state];
    NSURLSession *session = [NSURLSession sessionWithConfiguration:config delegate:delegate delegateQueue:nil];
    delegate.session = session;
    
    NSURLSessionDataTask *task = [session dataTaskWithURL:nsURL];
    [task resume];
    
    return std::make_shared<DownloadTaskIOS>(task, session, state);
}

void logInfo(const std::string& message) {
    os_log_info(logger, "%{public}s", message.c_str());
}

void logDebug(const std::string& message) {
    os_log_debug(logger, "%{public}s", message.c_str());
}

void logError(const std::string& message) {
    os_log_error(logger, "%{public}s", message.c_str());
}

} // namespace platform
} // namespace watermelondb
