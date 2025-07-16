#include "JSIAndroidBridgeModule.h"
#include "JSIAndroidUtils.h"

#include <jni.h>
#include <fbjni/fbjni.h>
#include <memory>
#include <sqlite3.h>

namespace facebook::react {

JSIAndroidBridgeModule::JSIAndroidBridgeModule(std::shared_ptr<CallInvoker> jsInvoker)
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {}

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
    return findDatabaseBridgeFromContext();
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

} // namespace facebook::react
