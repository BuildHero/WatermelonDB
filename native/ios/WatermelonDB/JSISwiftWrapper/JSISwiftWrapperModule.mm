#include "JSISwiftWrapperModule.h"
#include "JSIWrapperUtils.h"

#import <React/RCTBridge+Private.h>
#import <React/RCTBridgeModule.h>
#import <WatermelonDB-Swift.h>

namespace facebook::react {

JSISwiftWrapperModule::JSISwiftWrapperModule(std::shared_ptr<CallInvoker> jsInvoker)
: NativeWatermelonDBModuleCxxSpec(std::move(jsInvoker)) {}

jsi::Array JSISwiftWrapperModule::query(jsi::Runtime &rt, double tag, jsi::String table, jsi::String query) {
    RCTBridge *bridge = [RCTBridge currentBridge];
    DatabaseBridge *db = [bridge moduleForClass: DatabaseBridge.class];

    const std::lock_guard<std::mutex> lock(mutex_);
    
    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::query(db, rt, tagValue, table, query);

    return result.asObject(rt).asArray(rt);
}

jsi::Array JSISwiftWrapperModule::execSqlQuery(jsi::Runtime &rt, double tag, jsi::String sql, jsi::Array args) {
    RCTBridge *bridge = [RCTBridge currentBridge];
    DatabaseBridge *db = [bridge moduleForClass: DatabaseBridge.class];

    const std::lock_guard<std::mutex> lock(mutex_);

    // Convert double tag to jsi::Value
    jsi::Value tagValue = jsi::Value(tag);
    
    jsi::Value result = watermelondb::execSqlQuery(db, rt, tagValue, sql, args);
    
    return result.asObject(rt).asArray(rt);
}

} // namespace facebook::react
