#include "JSIAndroidBridgeModule.h"
#include "JSIAndroidUtils.h"

#include <jni.h>
#include <memory>
#include <sqlite3.h>

namespace facebook::react {

JSIAndroidBridgeModule::JSIAndroidBridgeModule(std::shared_ptr<CallInvoker> jsInvoker)
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {}

JNIEnv* JSIAndroidBridgeModule::getEnv() {
    return watermelondb::getEnv();
}

jobject JSIAndroidBridgeModule::getDatabaseBridge() {
    // TODO: Implement singleton pattern to get DatabaseBridge instance
    // This should get the bridge instance via singleton rather than storing it as a member
    return nullptr; // Placeholder - needs proper singleton implementation
}

jsi::Array JSIAndroidBridgeModule::query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query) {
    const std::lock_guard<std::mutex> lock(mutex_);
    
    jobject databaseBridge = getDatabaseBridge();
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::query(databaseBridge, rt, tagValue, table, query);
    
    return result.asObject(rt).asArray(rt);
}

jsi::Array JSIAndroidBridgeModule::execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args) {
    const std::lock_guard<std::mutex> lock(mutex_);
    
    jobject databaseBridge = getDatabaseBridge();
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::execSqlQuery(databaseBridge, rt, tagValue, sql, args);
    
    return result.asObject(rt).asArray(rt);
}

} // namespace facebook::react
