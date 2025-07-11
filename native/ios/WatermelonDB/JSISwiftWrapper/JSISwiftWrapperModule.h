#pragma once

#include <WatermelonDBSpecJSI.h>

#include <memory>
#include <string>
#include <mutex>

#import <jsi/jsi.h>

namespace facebook::react {

class JSISwiftWrapperModule : public NativeWatermelonDBModuleCxxSpec<JSISwiftWrapperModule> {
public:
    JSISwiftWrapperModule(std::shared_ptr<CallInvoker> jsInvoker);
    
    jsi::Array query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query);
    jsi::Array execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args);

private:
  std::mutex mutex_;  
};

} // namespace facebook::react
