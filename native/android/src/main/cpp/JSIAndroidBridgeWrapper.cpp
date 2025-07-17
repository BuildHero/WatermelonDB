//
// Created by BuildOpsLA27 on 9/30/24.
//
#include "JSIAndroidBridgeWrapper.h"
#include "JSIAndroidUtils.h"
#include <string>
#include <sqlite3.h>
#include <android/log.h>

using namespace watermelondb;

namespace watermelondb {

    JSIAndroidBridge::JSIAndroidBridge(jsi::Runtime *runtime, jobject bridge)
            : bridge_(bridge), runtime_(runtime) {
    }

    JSIAndroidBridge::~JSIAndroidBridge() {

    }

    jsi::Value JSIAndroidBridge::execSqlQuery(const jsi::Value &tag, const jsi::String &sql, const jsi::Array &arguments) {
        const std::lock_guard<std::mutex> lock(mutex_);
        return watermelondb::execSqlQuery(bridge_, *runtime_, tag, sql, arguments);
    }

    jsi::Value JSIAndroidBridge::query(const jsi::Value &tag, const jsi::String &table, const jsi::String &query) {
        const std::lock_guard<std::mutex> lock(mutex_);
        return watermelondb::query(bridge_, *runtime_, tag, table, query);
    }

    void JSIAndroidBridge::install(jsi::Runtime *runtime, jobject bridge) {
        auto androidBridge = std::make_shared<JSIAndroidBridge>(runtime, bridge);

        if (!runtime->global().hasProperty(*runtime, "WatermelonDB")) {
            jsi::Object watermelonDB = jsi::Object(*runtime);
            runtime->global().setProperty(*runtime, "WatermelonDB", std::move(watermelonDB));
        }

        auto query = jsi::Function::createFromHostFunction(
                *runtime,
                jsi::PropNameID::forAscii(*runtime, "query"),
                3,  // Number of arguments
                [androidBridge](jsi::Runtime &rt, const jsi::Value &thisValue, const jsi::Value *args, size_t count) -> jsi::Value {
                    if (count != 3) {
                        throw jsi::JSError(rt, "query requires 3 arguments (tag, table, query)");
                    }

                    return androidBridge->query(args[0], args[1].asString(rt), args[2].asString(rt));
                }
        );

        auto execQuery = jsi::Function::createFromHostFunction(
                *runtime,
                jsi::PropNameID::forAscii(*runtime, "execSqlQuery"),
                3,  // Number of arguments
                [androidBridge](jsi::Runtime &rt, const jsi::Value &thisValue, const jsi::Value *args, size_t count) -> jsi::Value {
                    if (count != 3) {
                        throw jsi::JSError(rt, "execSqlQuery requires 3 arguments (tag, sql, args)");
                    }

                    return androidBridge->execSqlQuery(args[0], args[1].asString(rt), args[2].asObject(rt).asArray(rt));
                }
        );

        runtime->global()
                .getPropertyAsObject(*runtime, "WatermelonDB")
                .setProperty(*runtime, "execSqlQuery", std::move(execQuery));

        runtime->global()
                .getPropertyAsObject(*runtime, "WatermelonDB")
                .setProperty(*runtime, "query", std::move(query));
    }

}