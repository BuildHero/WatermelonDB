#include "JSIAndroidBridgeModule.h"
#include "JSIAndroidUtils.h"
#include "SliceImportEngine.h"
#include "SliceImportDatabaseAdapterAndroid.h"

#include <jni.h>
#include <fbjni/fbjni.h>
#include <memory>
#include <sqlite3.h>
#include <ReactCommon/TurboModuleUtils.h>
#include <unordered_map>

namespace facebook::react {

namespace {
std::mutex gImportMutex;
std::unordered_map<void*, std::shared_ptr<watermelondb::SliceImportEngine>> gActiveImports;

void retainImport(const std::shared_ptr<watermelondb::SliceImportEngine>& engine) {
    std::lock_guard<std::mutex> lock(gImportMutex);
    gActiveImports[engine.get()] = engine;
}

void releaseImport(void* key) {
    std::lock_guard<std::mutex> lock(gImportMutex);
    gActiveImports.erase(key);
}
} // namespace

JSIAndroidBridgeModule::JSIAndroidBridgeModule(std::shared_ptr<CallInvoker> jsInvoker)
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {
    JNIEnv* env = getEnv();
    
    jobject localBridge = findDatabaseBridgeFromContext();
  
    if (localBridge == nullptr) {
        throw std::runtime_error("DatabaseBridge instance not available. Make sure the DatabaseBridge native module is initialized.");
    }
    
    globalDatabaseBridge_ = env->NewGlobalRef(localBridge);

    env->DeleteLocalRef(localBridge);
}

JSIAndroidBridgeModule::~JSIAndroidBridgeModule() {
    if (globalDatabaseBridge_ != nullptr) {
        getEnv()->DeleteGlobalRef(globalDatabaseBridge_);
        globalDatabaseBridge_ = nullptr;
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

} // namespace facebook::react
