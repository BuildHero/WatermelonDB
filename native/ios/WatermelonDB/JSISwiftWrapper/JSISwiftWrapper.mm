//
//  JSISwiftWrapper.mm
//  WatermelonDB
//
//  Created by BuildOpsLA27 on 9/27/24.
//  Copyright © 2024 Nozbe. All rights reserved.
//

#include "JSISwiftWrapper.h"
#include "JSIWrapperUtils.h"
#include "DatabaseUtils.h"
#include <string>

namespace watermelondb {

SwiftBridge::SwiftBridge(jsi::Runtime *runtime, DatabaseBridge *databaseBridge)
: runtime_(runtime), databaseBridge_(databaseBridge ) {
    
}

SwiftBridge::~SwiftBridge() {
    
}

jsi::Value SwiftBridge::query(const jsi::Value &tag, const jsi::String &table, const jsi::String &query) {
    const std::lock_guard<std::mutex> lock(mutex_);
    
    return watermelondb::query(databaseBridge_, *runtime_, tag, table, query);
}

jsi::Value SwiftBridge::execSqlQuery(const jsi::Value &tag, const jsi::String &sql, const jsi::Array &arguments) {
    const std::lock_guard<std::mutex> lock(mutex_);
    
    return watermelondb::execSqlQuery(databaseBridge_, *runtime_, tag, sql, arguments);
}

void SwiftBridge::install(jsi::Runtime *runtime, DatabaseBridge *databaseBridge) {
    // Create an instance of SwiftBridge
    auto swiftBridge = std::make_shared<SwiftBridge>(runtime, databaseBridge);
    
    if (!runtime->global().hasProperty(*runtime, "WatermelonDB")) {
        jsi::Object watermelonDB = jsi::Object(*runtime);
        runtime->global().setProperty(*runtime, "WatermelonDB", std::move(watermelonDB));
    }
    
    // Define the execSqlQuery function for JSI
    auto execSqlQueryFunc = jsi::Function::createFromHostFunction(
                                                                  *runtime,
                                                                  jsi::PropNameID::forAscii(*runtime, "execSqlQuery"),
                                                                  3,  // Number of arguments
                                                                  [swiftBridge](jsi::Runtime &rt, const jsi::Value &thisValue, const jsi::Value *args, size_t count) -> jsi::Value {
                                                                      if (count != 3) {
                                                                          throw jsi::JSError(rt, "execSqlQuery expects exactly 3 arguments.");
                                                                      }
                                                                      
                                                                      
                                                                      // Call the execSqlQuery function from SwiftBridge
                                                                      return swiftBridge->execSqlQuery(args[0], args[1].asString(rt), args[2].asObject(rt).asArray(rt));
                                                                  }
                                                                  );
    
    auto queryFunc = jsi::Function::createFromHostFunction(
                                                           *runtime,
                                                           jsi::PropNameID::forAscii(*runtime, "query"),
                                                           3,  // Number of arguments
                                                           [swiftBridge](jsi::Runtime &rt, const jsi::Value &thisValue, const jsi::Value *args, size_t count) -> jsi::Value {
                                                               if (count != 3) {
                                                                   throw jsi::JSError(rt, "query expects exactly 3 arguments.");
                                                               }
                                                               
                                                               
                                                               // Call the execSqlQuery function from SwiftBridge
                                                               return swiftBridge->query(args[0], args[1].asString(rt), args[2].asString(rt));
                                                           }
                                                           );
    
    
    // Set the functions in the global object
    
    runtime->global()
        .getPropertyAsObject(*runtime, "WatermelonDB")
        .setProperty(*runtime, "execSqlQuery", std::move(execSqlQueryFunc));
    
    runtime->global()
        .getPropertyAsObject(*runtime, "WatermelonDB")
        .setProperty(*runtime, "query", std::move(queryFunc));
}

}

