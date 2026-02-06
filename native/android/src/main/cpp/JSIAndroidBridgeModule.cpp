#include "JSIAndroidBridgeModule.h"
#include "JSIAndroidUtils.h"
#include "SliceImportEngine.h"
#include "SliceImportDatabaseAdapterAndroid.h"
#include "../../../../shared/SyncApplyEngine.h"
#include "../../../../shared/JsonUtils.h"

#include <jni.h>
#include <fbjni/fbjni.h>
#include <exception>
#include <memory>
#include <utility>
#include <sqlite3.h>
#include "SQLiteConnection.h"
#include <ReactCommon/TurboModuleUtils.h>
#include <unordered_map>
#include <cctype>

namespace facebook::react {

namespace {
std::mutex gImportMutex;
std::unordered_map<void*, std::shared_ptr<watermelondb::SliceImportEngine>> gActiveImports;
std::mutex gSocketMutex;
JSIAndroidBridgeModule* gSocketModule = nullptr;

void retainImport(const std::shared_ptr<watermelondb::SliceImportEngine>& engine) {
    std::lock_guard<std::mutex> lock(gImportMutex);
    gActiveImports[engine.get()] = engine;
}

void releaseImport(void* key) {
    std::lock_guard<std::mutex> lock(gImportMutex);
    gActiveImports.erase(key);
}
} // namespace

static void emitSocketEvent(const std::string &eventJson) {
    std::lock_guard<std::mutex> lock(gSocketMutex);
    if (!gSocketModule) {
        return;
    }
    gSocketModule->emitSyncEventFromNative(eventJson);
}

extern "C" JNIEXPORT void JNICALL
Java_com_nozbe_watermelondb_sync_SyncSocketManager_nativeOnStatus(
    JNIEnv* env,
    jclass,
    jint status,
    jstring errorMessage
) {
    std::string statusStr;
    switch (status) {
        case 0: statusStr = "connected"; break;
        case 1: statusStr = "disconnected"; break;
        default: statusStr = "error"; break;
    }

    std::string errorStr;
    if (errorMessage) {
        const char* chars = env->GetStringUTFChars(errorMessage, nullptr);
        if (chars) {
            errorStr = chars;
            env->ReleaseStringUTFChars(errorMessage, chars);
        }
    }

    std::string eventJson = std::string("{\"status\":\"") +
                            statusStr + "\"";
    if (!errorStr.empty()) {
        eventJson += std::string(",\"data\":\"") + watermelondb::json_utils::escapeJsonString(errorStr) + "\"";
    }
    eventJson += "}";

    emitSocketEvent(eventJson);
}

extern "C" JNIEXPORT void JNICALL
Java_com_nozbe_watermelondb_sync_SyncSocketManager_nativeOnCdc(
    JNIEnv*,
    jclass
) {
    emitSocketEvent("{\"status\":\"cdc\"}");
}

static sqlite3* acquireSqlite(jobject bridge, jint tag, std::string& errorMessage) {
    JNIEnv* env = facebook::jni::Environment::current();
    if (!env || !bridge) {
        errorMessage = "DatabaseBridge not available";
        return nullptr;
    }
    jclass cls = env->GetObjectClass(bridge);
    if (!cls) {
        env->ExceptionClear();
        errorMessage = "DatabaseBridge class not found";
        return nullptr;
    }
    jmethodID getConn = env->GetMethodID(cls, "getSQLiteConnection", "(I)J");
    if (!getConn) {
        env->ExceptionClear();
        env->DeleteLocalRef(cls);
        errorMessage = "getSQLiteConnection not found";
        return nullptr;
    }
    jlong ptr = env->CallLongMethod(bridge, getConn, tag);
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        errorMessage = "Failed to get SQLite connection";
        return nullptr;
    }
    if (!ptr) {
        errorMessage = "SQLite connection pointer is null";
        return nullptr;
    }
    auto connection = reinterpret_cast<SQLiteConnection*>(ptr);
    if (!connection || !connection->db) {
        errorMessage = "SQLite connection invalid";
        return nullptr;
    }
    return connection->db;
}

static void releaseSqlite(jobject bridge, jint tag) {
    JNIEnv* env = facebook::jni::Environment::current();
    if (!env || !bridge) {
        return;
    }
    jclass cls = env->GetObjectClass(bridge);
    if (!cls) {
        env->ExceptionClear();
        return;
    }
    jmethodID releaseConn = env->GetMethodID(cls, "releaseSQLiteConnection", "(I)V");
    if (releaseConn) {
        env->CallVoidMethod(bridge, releaseConn, tag);
    } else {
        env->ExceptionClear();
    }
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
}

JSIAndroidBridgeModule::JSIAndroidBridgeModule(std::shared_ptr<CallInvoker> jsInvoker)
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {
    {
        std::lock_guard<std::mutex> lock(gSocketMutex);
        gSocketModule = this;
    }
    syncEventState_ = std::make_shared<SyncEventState>();
    syncEventState_->jsInvoker = jsInvoker_;
    syncEngine_ = std::make_shared<watermelondb::SyncEngine>();
    syncEngine_->setEventCallback([this](const std::string &eventJson) {
        emitSyncEventLocked(eventJson);
    });
    syncEngine_->setApplyCallback([this](const std::string &payload, std::string &errorMessage) {
        if (syncConnectionTag_ <= 0) {
            errorMessage = "Missing connectionTag in sync config";
            return false;
        }
        jobject databaseBridge = getDatabaseBridge();
        if (!databaseBridge) {
            errorMessage = "DatabaseBridge not available";
            return false;
        }
        std::string error;
        sqlite3* db = acquireSqlite(databaseBridge, (jint)syncConnectionTag_, error);
        if (!db) {
            errorMessage = error;
            return false;
        }
        bool ok = watermelondb::applySyncPayload(db, payload, errorMessage);
        releaseSqlite(databaseBridge, (jint)syncConnectionTag_);
        return ok;
    });
    syncEngine_->setAuthTokenRequestCallback([this]() {
        requestAuthTokenFromJs();
    });
    syncEngine_->setPushChangesCallback([this](std::function<void(bool success, const std::string& errorMessage)> completion) {
        requestPushChangesFromJs(std::move(completion));
    });
    JNIEnv* env = getEnv();
    
    jobject localBridge = findDatabaseBridgeFromContext();
  
    if (localBridge == nullptr) {
        throw std::runtime_error("DatabaseBridge instance not available. Make sure the DatabaseBridge native module is initialized.");
    }
    
    globalDatabaseBridge_ = env->NewGlobalRef(localBridge);

    env->DeleteLocalRef(localBridge);
}

JSIAndroidBridgeModule::~JSIAndroidBridgeModule() {
    {
        std::lock_guard<std::mutex> lock(gSocketMutex);
        if (gSocketModule == this) {
            gSocketModule = nullptr;
        }
    }
    if (syncEngine_) {
        syncEngine_->shutdown();
    }
    if (globalDatabaseBridge_ != nullptr) {
        getEnv()->DeleteGlobalRef(globalDatabaseBridge_);
        globalDatabaseBridge_ = nullptr;
    }
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->alive = false;
        state->runtime = nullptr;
        state->listeners.clear();
    }
}

JNIEnv* JSIAndroidBridgeModule::getEnv() {
    return facebook::jni::Environment::current();
}

jobject JSIAndroidBridgeModule::findDatabaseBridgeFromContext() {
    JNIEnv* env = getEnv();
    
    // Find the DatabaseBridge class
    jclass databaseBridgeClass = env->FindClass("com/nozbe/watermelondb/DatabaseBridge");
    if (!databaseBridgeClass) {
        env->ExceptionClear();
        return nullptr;
    }
    
    // Get the Companion object field
    jfieldID companionField = env->GetStaticFieldID(
        databaseBridgeClass, 
        "Companion", 
        "Lcom/nozbe/watermelondb/DatabaseBridge$Companion;"
    );
    
    if (!companionField) {
        env->ExceptionClear();
        env->DeleteLocalRef(databaseBridgeClass);
        return nullptr;
    }
    
    // Get the Companion object instance
    jobject companionObj = env->GetStaticObjectField(databaseBridgeClass, companionField);
    
    if (!companionObj) {
        env->ExceptionClear();
        env->DeleteLocalRef(databaseBridgeClass);
        return nullptr;
    }
    
    // Find the Companion class
    jclass companionClass = env->FindClass("com/nozbe/watermelondb/DatabaseBridge$Companion");
    if (!companionClass) {
        env->ExceptionClear();
        env->DeleteLocalRef(databaseBridgeClass);
        env->DeleteLocalRef(companionObj);
        return nullptr;
    }
    
    // Get the getInstance method from the Companion class
    jmethodID getInstanceMethod = env->GetMethodID(
        companionClass, 
        "getInstance", 
        "()Lcom/nozbe/watermelondb/DatabaseBridge;"
    );
    
    if (!getInstanceMethod) {
        env->ExceptionClear();
        env->DeleteLocalRef(databaseBridgeClass);
        env->DeleteLocalRef(companionObj);
        env->DeleteLocalRef(companionClass);
        return nullptr;
    }
    
    // Call getInstance on the Companion object
    jobject bridgeInstance = env->CallObjectMethod(companionObj, getInstanceMethod);
    
    env->DeleteLocalRef(databaseBridgeClass);
    env->DeleteLocalRef(companionObj);
    env->DeleteLocalRef(companionClass);
    
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        return nullptr;
    }
    
    return bridgeInstance;
}

jobject JSIAndroidBridgeModule::getDatabaseBridge() {
    return globalDatabaseBridge_;
}

jsi::Array JSIAndroidBridgeModule::query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query) {
    const std::lock_guard<std::mutex> lock(mutex_);
    
    jobject databaseBridge = getDatabaseBridge();
    
    if (databaseBridge == nullptr) {
        throw jsi::JSError(rt, "DatabaseBridge instance not available. Make sure the DatabaseBridge native module is initialized.");
    }
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::query(databaseBridge, rt, tagValue, table, query);
    
    return result.asObject(rt).asArray(rt);
}

jsi::Array JSIAndroidBridgeModule::execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args) {
    const std::lock_guard<std::mutex> lock(mutex_);
    
    jobject databaseBridge = getDatabaseBridge();
    
    if (databaseBridge == nullptr) {
        throw jsi::JSError(rt, "DatabaseBridge instance not available. Make sure the DatabaseBridge native module is initialized.");
    }
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::execSqlQuery(databaseBridge, rt, tagValue, sql, args);
    
    return result.asObject(rt).asArray(rt);
}

jsi::Value JSIAndroidBridgeModule::importRemoteSlice(jsi::Runtime &rt, double tag, jsi::String sliceUrl) {
    const double tagCopy = tag;
    const std::string sliceUrlUtf8 = sliceUrl.utf8(rt);

    jobject databaseBridge = getDatabaseBridge();
    
    if (databaseBridge == nullptr) {
        throw jsi::JSError(rt, "DatabaseBridge instance not available. Make sure the DatabaseBridge native module is initialized.");
    }

    auto jsInvoker = jsInvoker_;

    return createPromiseAsJSIValue(rt, [databaseBridge, tagCopy, sliceUrlUtf8, jsInvoker](jsi::Runtime &rt2, std::shared_ptr<Promise> promise) {
        JNIEnv* env = watermelondb::getEnv();
        if (env) {
            watermelondb::configureJNI(env);
        }
        auto dbInterface = createAndroidDatabaseInterface(databaseBridge, static_cast<jint>(tagCopy));
        if (!dbInterface) {
            jsInvoker->invokeAsync([promise]() mutable {
                promise->reject("Failed to create Android database interface");
            });
            return;
        }

        auto engine = std::make_shared<watermelondb::SliceImportEngine>(dbInterface);
        void* engineKey = engine.get();
        retainImport(engine);

        engine->startImport(sliceUrlUtf8, [engineKey, jsInvoker, promise](const std::string& errorMessage) mutable {
            releaseImport(engineKey);
            jsInvoker->invokeAsync([promise, errorMessage]() mutable {
                if (!errorMessage.empty()) {
                    promise->reject(errorMessage);
                } else {
                    promise->resolve(jsi::Value::undefined());
                }
            });
        });
    });
}

void JSIAndroidBridgeModule::configureSync(jsi::Runtime &rt, jsi::String configJson) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    const std::string config = configJson.utf8(rt);
    const std::string tagKey = "\"connectionTag\"";
    size_t pos = config.find(tagKey);
    if (pos != std::string::npos) {
        pos = config.find(':', pos + tagKey.size());
        if (pos != std::string::npos) {
            pos++;
            while (pos < config.size() && std::isspace(static_cast<unsigned char>(config[pos]))) {
                pos++;
            }
            long long value = 0;
            bool any = false;
            while (pos < config.size() && std::isdigit(static_cast<unsigned char>(config[pos]))) {
                any = true;
                value = value * 10 + (config[pos] - '0');
                pos++;
            }
            if (any) {
                syncConnectionTag_ = value;
            }
        }
    }
    if (syncEngine_) {
        syncEngine_->configure(configJson.utf8(rt));
    }
}

void JSIAndroidBridgeModule::startSync(jsi::Runtime &rt, jsi::String reason) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->start(reason.utf8(rt));
    }
}

jsi::Value JSIAndroidBridgeModule::syncDatabaseAsync(jsi::Runtime &rt, jsi::String reason) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    auto engine = syncEngine_;
    auto jsInvoker = jsInvoker_;
    const std::string reasonUtf8 = reason.utf8(rt);
    
    return createPromiseAsJSIValue(rt, [engine, jsInvoker, reasonUtf8](jsi::Runtime &rt2, std::shared_ptr<Promise> promise) {
        if (!engine) {
            jsInvoker->invokeAsync([promise]() mutable {
                promise->reject("Sync engine not available");
            });
            return;
        }
        engine->startWithCompletion(reasonUtf8, [jsInvoker, promise](bool success, const std::string& errorMessage) mutable {
            jsInvoker->invokeAsync([promise, success, errorMessage]() mutable {
                if (!success) {
                    const std::string message = errorMessage.empty() ? "Sync failed" : errorMessage;
                    promise->reject(message);
                } else {
                    promise->resolve(jsi::Value::undefined());
                }
            });
        });
    });
}

void JSIAndroidBridgeModule::setSyncPullUrl(jsi::Runtime &rt, jsi::String pullEndpointUrl) {
    if (syncEngine_) {
        syncEngine_->setPullEndpointUrl(pullEndpointUrl.utf8(rt));
    }
}

jsi::String JSIAndroidBridgeModule::getSyncStateJson(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        return jsi::String::createFromUtf8(rt, syncEngine_->stateJson());
    }
    return jsi::String::createFromUtf8(rt, "{\"state\":\"idle\"}");
}

double JSIAndroidBridgeModule::addSyncListener(jsi::Runtime &rt, jsi::Function listener) {
    auto state = syncEventState_;
    if (!state) {
        return 0;
    }
    const std::lock_guard<std::mutex> lock(state->mutex);
    state->runtime = &rt;
    const int64_t id = nextSyncListenerId_++;
    state->listeners.emplace(id, std::move(listener));
    return static_cast<double>(id);
}

void JSIAndroidBridgeModule::removeSyncListener(jsi::Runtime &rt, double listenerId) {
    auto state = syncEventState_;
    if (!state) {
        return;
    }
    const std::lock_guard<std::mutex> lock(state->mutex);
    state->runtime = &rt;
    state->listeners.erase(static_cast<int64_t>(listenerId));
}

void JSIAndroidBridgeModule::setAuthToken(jsi::Runtime &rt, jsi::String token) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->setAuthToken(token.utf8(rt));
    }
}

void JSIAndroidBridgeModule::clearAuthToken(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    if (syncEngine_) {
        syncEngine_->clearAuthToken();
    }
}

void JSIAndroidBridgeModule::setAuthTokenProvider(jsi::Runtime &rt, jsi::Function provider) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
        authTokenProvider_ = std::make_shared<jsi::Function>(std::move(provider));
    }
    requestAuthTokenFromJs();
}

void JSIAndroidBridgeModule::setPushChangesProvider(jsi::Runtime &rt, jsi::Function provider) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
        pushChangesProvider_ = std::make_shared<jsi::Function>(std::move(provider));
    }
}

void JSIAndroidBridgeModule::initSyncSocket(jsi::Runtime &rt, jsi::String socketUrl) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    JNIEnv* env = getEnv();
    jclass cls = env->FindClass("com/nozbe/watermelondb/sync/SyncSocketManager");
    if (!cls) {
        env->ExceptionClear();
        return;
    }
    jmethodID method = env->GetStaticMethodID(cls, "initialize", "(Ljava/lang/String;)V");
    if (!method) {
        env->ExceptionClear();
        env->DeleteLocalRef(cls);
        return;
    }
    std::string url = socketUrl.utf8(rt);
    jstring jurl = env->NewStringUTF(url.c_str());
    env->CallStaticVoidMethod(cls, method, jurl);
    env->DeleteLocalRef(jurl);
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
}

void JSIAndroidBridgeModule::syncSocketAuthenticate(jsi::Runtime &rt, jsi::String token) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    JNIEnv* env = getEnv();
    jclass cls = env->FindClass("com/nozbe/watermelondb/sync/SyncSocketManager");
    if (!cls) {
        env->ExceptionClear();
        return;
    }
    jmethodID method = env->GetStaticMethodID(cls, "authenticate", "(Ljava/lang/String;)V");
    if (!method) {
        env->ExceptionClear();
        env->DeleteLocalRef(cls);
        return;
    }
    std::string tokenStr = token.utf8(rt);
    jstring jtoken = env->NewStringUTF(tokenStr.c_str());
    env->CallStaticVoidMethod(cls, method, jtoken);
    env->DeleteLocalRef(jtoken);
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
}

void JSIAndroidBridgeModule::syncSocketDisconnect(jsi::Runtime &rt) {
    auto state = syncEventState_;
    if (state) {
        const std::lock_guard<std::mutex> lock(state->mutex);
        state->runtime = &rt;
    }
    JNIEnv* env = getEnv();
    jclass cls = env->FindClass("com/nozbe/watermelondb/sync/SyncSocketManager");
    if (!cls) {
        env->ExceptionClear();
        return;
    }
    jmethodID method = env->GetStaticMethodID(cls, "disconnect", "()V");
    if (!method) {
        env->ExceptionClear();
        env->DeleteLocalRef(cls);
        return;
    }
    env->CallStaticVoidMethod(cls, method);
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
}

jsi::Value JSIAndroidBridgeModule::decompressZstd(jsi::Runtime &rt, jsi::String src, jsi::String dest) {
    const std::string srcUtf8 = src.utf8(rt);
    const std::string destUtf8 = dest.utf8(rt);

    auto jsInvoker = jsInvoker_;

    return createPromiseAsJSIValue(rt, [srcUtf8, destUtf8, jsInvoker](jsi::Runtime &rt2, std::shared_ptr<Promise> promise) {
        JNIEnv* env = watermelondb::getEnv();
        if (!env) {
            jsInvoker->invokeAsync([promise]() mutable {
                promise->reject("JNI environment not available");
            });
            return;
        }

        jclass cls = env->FindClass("com/nozbe/watermelondb/ZstdFileUtil");
        if (!cls) {
            env->ExceptionClear();
            jsInvoker->invokeAsync([promise]() mutable {
                promise->reject("ZstdFileUtil class not found");
            });
            return;
        }

        jmethodID method = env->GetStaticMethodID(cls, "decompressZstd", "(Ljava/lang/String;Ljava/lang/String;)V");
        if (!method) {
            env->ExceptionClear();
            env->DeleteLocalRef(cls);
            jsInvoker->invokeAsync([promise]() mutable {
                promise->reject("decompressZstd method not found");
            });
            return;
        }

        jstring jsrc = env->NewStringUTF(srcUtf8.c_str());
        jstring jdest = env->NewStringUTF(destUtf8.c_str());
        env->CallStaticVoidMethod(cls, method, jsrc, jdest);

        std::string errorMessage;
        if (env->ExceptionCheck()) {
            jthrowable exception = env->ExceptionOccurred();
            env->ExceptionClear();
            if (exception) {
                jclass throwableClass = env->FindClass("java/lang/Throwable");
                jmethodID getMessageMethod = env->GetMethodID(throwableClass, "getMessage", "()Ljava/lang/String;");
                jstring jmsg = (jstring)env->CallObjectMethod(exception, getMessageMethod);
                if (jmsg) {
                    const char* chars = env->GetStringUTFChars(jmsg, nullptr);
                    if (chars) {
                        errorMessage = chars;
                        env->ReleaseStringUTFChars(jmsg, chars);
                    }
                    env->DeleteLocalRef(jmsg);
                }
                env->DeleteLocalRef(throwableClass);
                env->DeleteLocalRef(exception);
            }
            if (errorMessage.empty()) {
                errorMessage = "Zstd decompression failed";
            }
        }

        env->DeleteLocalRef(jsrc);
        env->DeleteLocalRef(jdest);
        env->DeleteLocalRef(cls);

        jsInvoker->invokeAsync([promise, errorMessage]() mutable {
            if (!errorMessage.empty()) {
                promise->reject(errorMessage);
            } else {
                promise->resolve(jsi::Value::undefined());
            }
        });
    });
}

void JSIAndroidBridgeModule::emitSyncEventFromNative(const std::string &eventJson) {
    emitSyncEventLocked(eventJson);
}

void JSIAndroidBridgeModule::emitSyncEventLocked(const std::string &eventJson) {
    auto state = syncEventState_;
    if (!state || !state->jsInvoker) {
        return;
    }
    auto jsInvoker = state->jsInvoker;
    jsInvoker->invokeAsync([state, eventJson]() {
        const std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->alive || !state->runtime || state->listeners.empty()) {
            return;
        }
        jsi::Runtime &rt = *state->runtime;
        for (auto &entry : state->listeners) {
            entry.second.call(rt, jsi::String::createFromUtf8(rt, eventJson));
        }
    });
}

void JSIAndroidBridgeModule::requestAuthTokenFromJs() {
    auto state = syncEventState_;
    if (!state || !state->jsInvoker) {
        return;
    }
    auto engineWeak = std::weak_ptr<watermelondb::SyncEngine>(syncEngine_);
    state->jsInvoker->invokeAsync([state, engineWeak, this]() {
        jsi::Runtime* runtime = nullptr;
        std::shared_ptr<jsi::Function> provider;
        {
            const std::lock_guard<std::mutex> lock(state->mutex);
            if (!state->alive || !state->runtime) {
                return;
            }
            runtime = state->runtime;
            provider = authTokenProvider_;
        }
        if (!runtime || !provider) {
            return;
        }
        jsi::Runtime& rt = *runtime;
        jsi::Value result;
        try {
            result = provider->call(rt);
        } catch (...) {
            if (auto engine = engineWeak.lock()) {
                engine->clearAuthToken();
            }
            return;
        }

        if (result.isString()) {
            if (auto engine = engineWeak.lock()) {
                engine->setAuthToken(result.asString(rt).utf8(rt));
            }
            return;
        }
        if (!result.isObject()) {
            if (auto engine = engineWeak.lock()) {
                engine->clearAuthToken();
            }
            return;
        }

        jsi::Object promiseObj = result.asObject(rt);
        if (!promiseObj.hasProperty(rt, "then")) {
            if (auto engine = engineWeak.lock()) {
                engine->clearAuthToken();
            }
            return;
        }

        jsi::Value thenVal = promiseObj.getProperty(rt, "then");
        if (!thenVal.isObject() || !thenVal.asObject(rt).isFunction(rt)) {
            if (auto engine = engineWeak.lock()) {
                engine->clearAuthToken();
            }
            return;
        }

        jsi::Function thenFn = thenVal.asObject(rt).asFunction(rt);

        auto onFulfilled = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "onFulfilled"),
            1,
            [engineWeak](jsi::Runtime& rt2, const jsi::Value&, const jsi::Value* argv, size_t argc) -> jsi::Value {
                if (argc >= 1 && argv[0].isString()) {
                    if (auto engine = engineWeak.lock()) {
                        engine->setAuthToken(argv[0].asString(rt2).utf8(rt2));
                    }
                } else if (auto engine = engineWeak.lock()) {
                    engine->clearAuthToken();
                }
                return jsi::Value::undefined();
            });

        auto onRejected = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "onRejected"),
            1,
            [engineWeak](jsi::Runtime& rt2, const jsi::Value&, const jsi::Value* argv, size_t argc) -> jsi::Value {
                if (auto engine = engineWeak.lock()) {
                    engine->clearAuthToken();
                }
                return jsi::Value::undefined();
            });

        thenFn.callWithThis(rt, promiseObj, onFulfilled, onRejected);
    });
}

void JSIAndroidBridgeModule::requestPushChangesFromJs(
    std::function<void(bool success, const std::string& errorMessage)> completion) {
    auto state = syncEventState_;
    if (!state || !state->jsInvoker) {
        completion(false, "Missing JS runtime for pushChanges");
        return;
    }
    auto completionPtr = std::make_shared<std::function<void(bool, const std::string&)>>(std::move(completion));
    state->jsInvoker->invokeAsync([state, completionPtr, this]() {
        jsi::Runtime* runtime = nullptr;
        std::shared_ptr<jsi::Function> provider;
        {
            const std::lock_guard<std::mutex> lock(state->mutex);
            if (!state->alive || !state->runtime) {
                (*completionPtr)(false, "Missing JS runtime for pushChanges");
                return;
            }
            runtime = state->runtime;
            provider = pushChangesProvider_;
        }
        if (!runtime || !provider) {
            (*completionPtr)(false, "Missing pushChanges provider");
            return;
        }
        jsi::Runtime& rt = *runtime;
        jsi::Value result;
        try {
            result = provider->call(rt);
        } catch (const jsi::JSError& e) {
            (*completionPtr)(false, e.what());
            return;
        } catch (const std::exception& e) {
            (*completionPtr)(false, e.what());
            return;
        } catch (...) {
            (*completionPtr)(false, "pushChangesProvider threw");
            return;
        }
        if (result.isObject()) {
            try {
                jsi::Object resultObj = result.asObject(rt);
                jsi::Value thenValue = resultObj.getProperty(rt, "then");
                if (thenValue.isObject() && thenValue.asObject(rt).isFunction(rt)) {
                    jsi::Function thenFunc = thenValue.asObject(rt).asFunction(rt);
                    auto resolve = jsi::Function::createFromHostFunction(
                        rt,
                        jsi::PropNameID::forUtf8(rt, "resolve"),
                        1,
                        [completionPtr](jsi::Runtime&, const jsi::Value&, const jsi::Value*, size_t) -> jsi::Value {
                            (*completionPtr)(true, "");
                            return jsi::Value::undefined();
                        });
                    auto reject = jsi::Function::createFromHostFunction(
                        rt,
                        jsi::PropNameID::forUtf8(rt, "reject"),
                        1,
                        [completionPtr](jsi::Runtime& rt2, const jsi::Value&, const jsi::Value* args, size_t count) -> jsi::Value {
                            std::string message = "pushChanges rejected";
                            if (count > 0 && args[0].isString()) {
                                message = args[0].asString(rt2).utf8(rt2);
                            }
                            (*completionPtr)(false, message);
                            return jsi::Value::undefined();
                        });
                    thenFunc.callWithThis(rt, resultObj, resolve, reject);
                    return;
                }
            } catch (const jsi::JSError& e) {
                (*completionPtr)(false, e.what());
                return;
            } catch (const std::exception& e) {
                (*completionPtr)(false, e.what());
                return;
            } catch (...) {
                (*completionPtr)(false, "pushChangesProvider promise handling threw");
                return;
            }
        }
        (*completionPtr)(true, "");
    });
}

} // namespace facebook::react
