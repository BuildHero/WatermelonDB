//
//  JSIWrapperUtils.h
//  WatermelonDB
//
//  Created by BuildOpsLA27 on 10/4/24.
//

#ifndef JSIWrapperUtils_h
#define JSIWrapperUtils_h

#import <jsi/jsi.h>
#import <React/RCTEventEmitter.h>
#import <React/RCTBridgeModule.h>
#import "WatermelonDB-Swift.h"

using namespace facebook;

namespace watermelondb {
    jsi::Value execSqlQuery(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &args);
    jsi::Value query(DatabaseBridge *databaseBridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &table, const jsi::String &query);
} // namespace watermelondb

#endif /* JSIWrapperUtils_h */
