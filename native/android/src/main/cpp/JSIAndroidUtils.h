//
//  JSIAndroidUtils.h
//  WatermelonDB
//
//  Created by BuildOpsLA27 on 10/4/24.
//

#ifndef JSIAndroidUtils_h
#define JSIAndroidUtils_h

#include <jsi/jsi.h>
#include <jni.h>

#ifndef LOG_TAG
#define LOG_TAG "WatermelonDB"
#endif

using namespace facebook;

namespace watermelondb {
    jsi::Value execSqlQuery(jobject bridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &arguments);
    jsi::Value query(jobject bridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &table, const jsi::String &query);
    
    JNIEnv* getEnv();
    JNIEnv* attachCurrentThread();
    bool waitForJvm(int timeoutMs);
    void configureJNI(JNIEnv *env);
} // namespace watermelondb

#endif /* JSIAndroidUtils_h */ 
