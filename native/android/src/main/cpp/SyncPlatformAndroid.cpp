#include "SyncPlatform.h"
#include "JSIAndroidUtils.h"
#include "SlicePlatformAndroidQueue.h"

#include <jni.h>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace {
constexpr const char* kSyncHttpManagerClass = "com/nozbe/watermelondb/sync/SyncHttpManager";
constexpr const char* kStartRequestSig = "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[BIJ)V";

struct HttpCallbackState {
    std::function<void(const watermelondb::platform::HttpResponse&)> onComplete;
    std::atomic<bool> completed{false};
};

std::mutex gHttpMutex;
std::unordered_map<int64_t, std::shared_ptr<HttpCallbackState>> gHttpCallbacks;
std::atomic<int64_t> gNextHttpHandle{1};

// Cached global ref to com.nozbe.watermelondb.sync.SyncHttpManager.
//
// FindClass for app classes only works from threads that descend from a Java
// caller (so the thread's class loader stack reaches the app loader). Native
// threads attached via AttachCurrentThread (e.g. retry threads spawned by
// SyncEngine::scheduleRetryLocked) get the system class loader, which can't
// see app classes — FindClass returns null and the sync surfaces as
// "Failed to start HTTP request". We resolve the class once from a Java-side
// thread and reuse the global ref forever.
std::atomic<jclass> gSyncHttpManagerClass{nullptr};

jclass getSyncHttpManagerClass(JNIEnv* env) {
    jclass cached = gSyncHttpManagerClass.load(std::memory_order_acquire);
    if (cached) {
        return cached;
    }
    jclass local = env->FindClass(kSyncHttpManagerClass);
    if (!local) {
        env->ExceptionClear();
        return nullptr;
    }
    jclass global = (jclass)env->NewGlobalRef(local);
    env->DeleteLocalRef(local);
    if (!global) {
        return nullptr;
    }
    jclass expected = nullptr;
    if (!gSyncHttpManagerClass.compare_exchange_strong(
            expected, global, std::memory_order_acq_rel, std::memory_order_acquire)) {
        // Lost the race — another thread cached first; drop our duplicate ref.
        env->DeleteGlobalRef(global);
        return expected;
    }
    return global;
}

jmethodID getStaticMethod(JNIEnv* env, jclass cls, const char* name, const char* sig) {
    jmethodID method = env->GetStaticMethodID(cls, name, sig);
    if (!method) {
        env->ExceptionClear();
    }
    return method;
}

bool callStartRequest(JNIEnv* env,
                      const std::string& url,
                      const std::string& method,
                      const std::vector<std::string>& headerKeys,
                      const std::vector<std::string>& headerValues,
                      const std::string& body,
                      int timeoutMs,
                      int64_t handle) {
    jclass cls = getSyncHttpManagerClass(env);
    if (!cls) {
        return false;
    }
    jmethodID methodId = getStaticMethod(env, cls, "startRequest", kStartRequestSig);
    if (!methodId) {
        return false;
    }

    jstring jUrl = env->NewStringUTF(url.c_str());
    jstring jMethod = env->NewStringUTF(method.c_str());

    jclass stringClass = env->FindClass("java/lang/String");
    jobjectArray jKeys = env->NewObjectArray((jsize)headerKeys.size(), stringClass, nullptr);
    jobjectArray jValues = env->NewObjectArray((jsize)headerValues.size(), stringClass, nullptr);
    for (size_t i = 0; i < headerKeys.size(); i++) {
        env->SetObjectArrayElement(jKeys, (jsize)i, env->NewStringUTF(headerKeys[i].c_str()));
        env->SetObjectArrayElement(jValues, (jsize)i, env->NewStringUTF(headerValues[i].c_str()));
    }

    jbyteArray jBody = nullptr;
    if (!body.empty()) {
        jBody = env->NewByteArray((jsize)body.size());
        env->SetByteArrayRegion(jBody, 0, (jsize)body.size(), (const jbyte*)body.data());
    }

    env->CallStaticVoidMethod(cls, methodId, jUrl, jMethod, jKeys, jValues, jBody, (jint)timeoutMs, (jlong)handle);

    env->DeleteLocalRef(jUrl);
    env->DeleteLocalRef(jMethod);
    env->DeleteLocalRef(jKeys);
    env->DeleteLocalRef(jValues);
    if (jBody) {
        env->DeleteLocalRef(jBody);
    }

    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        return false;
    }
    return true;
}

extern "C" JNIEXPORT void JNICALL
Java_com_nozbe_watermelondb_sync_SyncHttpManager_nativeOnComplete(
    JNIEnv* env,
    jclass,
    jlong handle,
    jint statusCode,
    jstring body,
    jstring errorMessage
) {
    watermelondb::configureJNI(env);
    std::shared_ptr<HttpCallbackState> state;
    {
        std::lock_guard<std::mutex> lock(gHttpMutex);
        auto it = gHttpCallbacks.find(handle);
        if (it == gHttpCallbacks.end()) {
            return;
        }
        state = it->second;
        gHttpCallbacks.erase(it);
    }
    if (!state || state->completed.exchange(true)) {
        return;
    }
    std::string bodyStr;
    if (body) {
        const char* chars = env->GetStringUTFChars(body, nullptr);
        if (chars) {
            bodyStr = chars;
            env->ReleaseStringUTFChars(body, chars);
        }
    }
    std::string errorStr;
    if (errorMessage) {
        const char* chars = env->GetStringUTFChars(errorMessage, nullptr);
        if (chars) {
            errorStr = chars;
            env->ReleaseStringUTFChars(errorMessage, chars);
        }
    }
    watermelondb::platform::HttpResponse resp;
    resp.statusCode = (int)statusCode;
    resp.body = bodyStr;
    resp.errorMessage = errorStr;

    watermelondb::android::runOnWorkQueue([state, resp]() {
        state->onComplete(resp);
    });
}

} // namespace

namespace watermelondb {
namespace platform {

void httpRequest(const HttpRequest& request,
                 std::function<void(const HttpResponse&)> onComplete) {
    JNIEnv* env = getEnv();
    if (!env) {
        HttpResponse resp;
        resp.errorMessage = "JNI env not available";
        onComplete(resp);
        return;
    }
    watermelondb::configureJNI(env);

    auto state = std::make_shared<HttpCallbackState>();
    state->onComplete = std::move(onComplete);

    int64_t handle = gNextHttpHandle.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(gHttpMutex);
        gHttpCallbacks[handle] = state;
    }

    std::vector<std::string> headerKeys;
    std::vector<std::string> headerValues;
    headerKeys.reserve(request.headers.size());
    headerValues.reserve(request.headers.size());
    for (const auto& entry : request.headers) {
        headerKeys.push_back(entry.first);
        headerValues.push_back(entry.second);
    }

    if (!callStartRequest(env,
                          request.url,
                          request.method.empty() ? "GET" : request.method,
                          headerKeys,
                          headerValues,
                          request.body,
                          request.timeoutMs,
                          handle)) {
        {
            std::lock_guard<std::mutex> lock(gHttpMutex);
            gHttpCallbacks.erase(handle);
        }
        HttpResponse resp;
        resp.errorMessage = "Failed to start HTTP request";
        state->onComplete(resp);
    }
}

std::string generateRequestId() {
    JNIEnv* env = getEnv();
    if (!env) {
        return "";
    }
    watermelondb::configureJNI(env);

    jclass uuidClass = env->FindClass("java/util/UUID");
    if (!uuidClass) {
        env->ExceptionClear();
        return "";
    }
    jmethodID randomMethod = env->GetStaticMethodID(uuidClass, "randomUUID", "()Ljava/util/UUID;");
    if (!randomMethod) {
        env->ExceptionClear();
        env->DeleteLocalRef(uuidClass);
        return "";
    }
    jobject uuidObj = env->CallStaticObjectMethod(uuidClass, randomMethod);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        env->DeleteLocalRef(uuidClass);
        return "";
    }
    jmethodID toStringMethod = env->GetMethodID(uuidClass, "toString", "()Ljava/lang/String;");
    if (!toStringMethod) {
        env->ExceptionClear();
        env->DeleteLocalRef(uuidClass);
        if (uuidObj) {
            env->DeleteLocalRef(uuidObj);
        }
        return "";
    }
    jstring uuidStr = (jstring)env->CallObjectMethod(uuidObj, toStringMethod);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        env->DeleteLocalRef(uuidClass);
        if (uuidObj) {
            env->DeleteLocalRef(uuidObj);
        }
        return "";
    }
    std::string result;
    if (uuidStr) {
        const char* chars = env->GetStringUTFChars(uuidStr, nullptr);
        if (chars) {
            result = chars;
            env->ReleaseStringUTFChars(uuidStr, chars);
        }
        env->DeleteLocalRef(uuidStr);
    }
    env->DeleteLocalRef(uuidObj);
    env->DeleteLocalRef(uuidClass);
    return result;
}

} // namespace platform
} // namespace watermelondb
