#include "SlicePlatform.h"
#include "JSIAndroidUtils.h"
#include "SlicePlatformAndroidQueue.h"

#include <android/log.h>
#include <jni.h>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <condition_variable>
#include <future>
#include <thread>
#include <unistd.h>

namespace {
constexpr const char* kSliceDownloadManagerClass = "com/nozbe/watermelondb/slice/SliceDownloadManager";
constexpr const char* kStartDownloadSig = "(Ljava/lang/String;J)V";
constexpr const char* kCancelDownloadSig = "(J)V";

struct DownloadCallbackState {
    std::function<void(const uint8_t* data, size_t length)> onData;
    std::function<void(const std::string& errorMessage)> onComplete;
    std::atomic<bool> completed{false};
};

std::mutex gDownloadMutex;
std::unordered_map<int64_t, std::shared_ptr<DownloadCallbackState>> gDownloadCallbacks;
std::atomic<int64_t> gNextHandle{1};

std::mutex gWorkMutex;
std::condition_variable gWorkCv;
std::queue<std::function<void()>> gWorkQueue;
std::atomic<bool> gWorkThreadStarted{false};
std::thread gWorkThread;
std::mutex gWorkThreadIdMutex;
std::thread::id gWorkThreadId;

void startWorkThread() {
    bool expected = false;
    if (!gWorkThreadStarted.compare_exchange_strong(expected, true)) {
        return;
    }
    gWorkThread = std::thread([]() {
        {
            std::lock_guard<std::mutex> lock(gWorkThreadIdMutex);
            gWorkThreadId = std::this_thread::get_id();
        }
        if (!watermelondb::waitForJvm(5000) || !watermelondb::attachCurrentThread()) {
            __android_log_print(ANDROID_LOG_ERROR, "WatermelonDB",
                                "Failed to attach work queue thread to JVM");
            {
                std::lock_guard<std::mutex> lock(gWorkThreadIdMutex);
                gWorkThreadId = std::thread::id();
            }
            gWorkThreadStarted.store(false);
            return;
        }
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(gWorkMutex);
                gWorkCv.wait(lock, [] { return !gWorkQueue.empty(); });
                task = std::move(gWorkQueue.front());
                gWorkQueue.pop();
            }
            task();
        }
    });
    gWorkThread.detach();
}

jclass getSliceDownloadManagerClass(JNIEnv* env) {
    jclass local = env->FindClass(kSliceDownloadManagerClass);
    if (!local) {
        env->ExceptionClear();
        return nullptr;
    }
    jclass global = (jclass)env->NewGlobalRef(local);
    env->DeleteLocalRef(local);
    return global;
}

jmethodID getStaticMethod(JNIEnv* env, jclass cls, const char* name, const char* sig) {
    jmethodID method = env->GetStaticMethodID(cls, name, sig);
    if (!method) {
        env->ExceptionClear();
    }
    return method;
}

bool callStartDownload(JNIEnv* env, const std::string& url, int64_t handle) {
    jclass cls = getSliceDownloadManagerClass(env);
    if (!cls) {
        return false;
    }
    jmethodID method = getStaticMethod(env, cls, "startDownload", kStartDownloadSig);
    if (!method) {
        env->DeleteGlobalRef(cls);
        return false;
    }
    jstring jUrl = env->NewStringUTF(url.c_str());
    env->CallStaticVoidMethod(cls, method, jUrl, (jlong)handle);
    env->DeleteLocalRef(jUrl);
    env->DeleteGlobalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        return false;
    }
    return true;
}

void callCancelDownload(JNIEnv* env, int64_t handle) {
    jclass cls = getSliceDownloadManagerClass(env);
    if (!cls) {
        return;
    }
    jmethodID method = getStaticMethod(env, cls, "cancelDownload", kCancelDownloadSig);
    if (!method) {
        env->DeleteGlobalRef(cls);
        return;
    }
    env->CallStaticVoidMethod(cls, method, (jlong)handle);
    env->DeleteGlobalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
}

} // namespace

extern "C" JNIEXPORT void JNICALL
Java_com_nozbe_watermelondb_slice_SliceDownloadManager_nativeOnData(
    JNIEnv* env,
    jclass,
    jlong handle,
    jbyteArray data,
    jint length
) {
    watermelondb::configureJNI(env);
    if (!data || length <= 0) {
        return;
    }

    std::shared_ptr<DownloadCallbackState> state;
    {
        std::lock_guard<std::mutex> lock(gDownloadMutex);
        auto it = gDownloadCallbacks.find(static_cast<int64_t>(handle));
        if (it == gDownloadCallbacks.end()) {
            return;
        }
        state = it->second;
    }

    if (!state || state->completed.load()) {
        return;
    }

    jbyte* bytes = env->GetByteArrayElements(data, nullptr);
    if (!bytes) {
        return;
    }
    std::vector<uint8_t> copy(bytes, bytes + length);
    env->ReleaseByteArrayElements(data, bytes, JNI_ABORT);

    watermelondb::android::runOnWorkQueue([state, copy = std::move(copy)]() {
        if (!state || state->completed.load()) {
            return;
        }
        state->onData(copy.data(), copy.size());
    });
}

extern "C" JNIEXPORT void JNICALL
Java_com_nozbe_watermelondb_slice_SliceDownloadManager_nativeOnComplete(
    JNIEnv* env,
    jclass,
    jlong handle,
    jstring errorMessage
) {
    watermelondb::configureJNI(env);
    std::shared_ptr<DownloadCallbackState> state;
    {
        std::lock_guard<std::mutex> lock(gDownloadMutex);
        auto it = gDownloadCallbacks.find(static_cast<int64_t>(handle));
        if (it == gDownloadCallbacks.end()) {
            return;
        }
        state = it->second;
        gDownloadCallbacks.erase(it);
    }

    if (!state || state->completed.exchange(true)) {
        return;
    }

    std::string error;
    if (errorMessage) {
        const char* chars = env->GetStringUTFChars(errorMessage, nullptr);
        if (chars) {
            error = chars;
            env->ReleaseStringUTFChars(errorMessage, chars);
        }
    }
    watermelondb::android::runOnWorkQueue([state, error]() {
        state->onComplete(error);
    });
}

namespace watermelondb {
namespace android {

void runOnWorkQueue(const std::function<void()>& work) {
    startWorkThread();
    {
        std::lock_guard<std::mutex> lock(gWorkMutex);
        gWorkQueue.push(work);
    }
    gWorkCv.notify_one();
}

bool isOnWorkQueue() {
    std::lock_guard<std::mutex> lock(gWorkThreadIdMutex);
    return gWorkThreadStarted.load() && std::this_thread::get_id() == gWorkThreadId;
}

} // namespace android
} // namespace watermelondb

namespace watermelondb {
namespace platform {

static constexpr const char* kLogTag = "WatermelonDB";

class DownloadTaskAndroid : public DownloadHandle {
public:
    explicit DownloadTaskAndroid(int64_t handle) : handle_(handle) {}

    void cancel() override {
        JNIEnv* env = getEnv();
        if (env) {
            callCancelDownload(env, handle_);
        }
    }

private:
    int64_t handle_;
};

class MemoryAlertHandleAndroid : public MemoryAlertHandle {
public:
    void cancel() override {}
};

void initializeWorkQueue() {
    // No-op on Android: work queue starts on first runOnWorkQueue after JVM is available
}

unsigned long calculateOptimalBatchSize() {
    unsigned long long physicalMemory = 0;
    long pages = sysconf(_SC_PHYS_PAGES);
    long pageSize = sysconf(_SC_PAGE_SIZE);
    if (pages > 0 && pageSize > 0) {
        physicalMemory = static_cast<unsigned long long>(pages) * static_cast<unsigned long long>(pageSize);
    }

    unsigned int cores = std::thread::hardware_concurrency();
    unsigned long batchSize = 500;

    if (physicalMemory >= 6ULL * 1024 * 1024 * 1024) {
        batchSize = 2000;
    } else if (physicalMemory >= 4ULL * 1024 * 1024 * 1024) {
        batchSize = 1500;
    } else if (physicalMemory >= 3ULL * 1024 * 1024 * 1024) {
        batchSize = 1000;
    } else if (physicalMemory >= 2ULL * 1024 * 1024 * 1024) {
        batchSize = 500;
    } else {
        batchSize = 250;
    }

    if (cores > 0 && cores <= 2) {
        batchSize /= 2;
    }

    __android_log_print(ANDROID_LOG_INFO, kLogTag,
                        "Device: %.1f GB RAM, %u cores → initial batch size: %lu",
                        physicalMemory / (1024.0 * 1024.0 * 1024.0),
                        cores,
                        batchSize);

    return batchSize;
}

std::shared_ptr<MemoryAlertHandle> setupMemoryAlertCallback(const std::function<void(MemoryAlertLevel)>&) {
    return std::make_shared<MemoryAlertHandleAndroid>();
}

void cancelMemoryPressureMonitoring() {
    // No-op
}

std::shared_ptr<DownloadHandle> downloadFile(
    const std::string& url,
    std::function<void(const uint8_t* data, size_t length)> onData,
    std::function<void(const std::string& errorMessage)> onComplete
) {
    JNIEnv* env = getEnv();
    if (!env) {
        onComplete("JNI env not available");
        return nullptr;
    }

    auto state = std::make_shared<DownloadCallbackState>();
    state->onData = std::move(onData);
    state->onComplete = std::move(onComplete);

    int64_t handle = gNextHandle.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(gDownloadMutex);
        gDownloadCallbacks[handle] = state;
    }

    if (!callStartDownload(env, url, handle)) {
        {
            std::lock_guard<std::mutex> lock(gDownloadMutex);
            gDownloadCallbacks.erase(handle);
        }
        state->onComplete("Failed to start download");
        return nullptr;
    }

    return std::make_shared<DownloadTaskAndroid>(handle);
}

void logInfo(const std::string& message) {
    __android_log_print(ANDROID_LOG_INFO, kLogTag, "%s", message.c_str());
}

void logDebug(const std::string& message) {
    __android_log_print(ANDROID_LOG_DEBUG, kLogTag, "%s", message.c_str());
}

void logError(const std::string& message) {
    __android_log_print(ANDROID_LOG_ERROR, kLogTag, "%s", message.c_str());
}

} // namespace platform
} // namespace watermelondb
