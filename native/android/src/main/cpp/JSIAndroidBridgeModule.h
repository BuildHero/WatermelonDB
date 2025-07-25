#pragma once

#include <WatermelonDBSpecJSI.h>

#include <memory>
#include <string>
#include <mutex>
#include <jni.h>

#include <jsi/jsi.h>

namespace facebook::react {

class JSIAndroidBridgeModule : public NativeWatermelonDBModuleCxxSpec<JSIAndroidBridgeModule> {
public:
    JSIAndroidBridgeModule(std::shared_ptr<CallInvoker> jsInvoker);
    ~JSIAndroidBridgeModule();
    
    jsi::Array query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query);
    jsi::Array execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args);

private:
    std::mutex mutex_;
    
    jobject globalDatabaseBridge_ = nullptr;
    
    JNIEnv* getEnv();
    jobject getDatabaseBridge();
    jobject findDatabaseBridgeFromContext();
};

} // namespace facebook::react
