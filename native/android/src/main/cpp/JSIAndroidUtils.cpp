#include "JSIAndroidUtils.h"
#include "../../../../shared/DatabaseUtils.h"
#include <string>
#include <fbjni/fbjni.h>
#include <sqlite3.h>
#include <android/log.h>

using namespace watermelondb;

struct SQLiteConnection {
    sqlite3* const db;
    const int openFlags;
    char* path;
    char* label;

    volatile bool canceled;

    SQLiteConnection(sqlite3* db, int openFlags, const char* path_, const char* label_) :
            db(db), openFlags(openFlags), canceled(false) {
        path = strdup(path_);
        label = strdup(label_);
    }

    ~SQLiteConnection() {
        free(path);
        free(label);
    }
};

namespace watermelondb {
    namespace platform {
        void consoleLog(std::string message) {
            __android_log_print(ANDROID_LOG_INFO, LOG_TAG, "%s\n", message.c_str());
        }

        void consoleError(std::string message) {
            __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, "%s\n", message.c_str());
        }

        void initializeSqlite() {
            // Nothing to do
        }

        std::string resolveDatabasePath(std::string path) {
            return std::string();
        }
    }

    JNIEnv* getEnv() {
        return facebook::jni::Environment::current();
    }

    jsi::Value execSqlQuery(jobject bridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &arguments) {
        JNIEnv *env = getEnv();
        jint jTag = static_cast<jint>(tag.asNumber());

        std::string queryStr = sql.utf8(rt);

        jclass myNativeModuleClass = env->GetObjectClass(bridge);

        jmethodID getConnectionMethod = env->GetMethodID(
                myNativeModuleClass,
                "getSQLiteConnection",
                "(I)J"
        );

        jmethodID releaseConnectionMethod = env->GetMethodID(
                myNativeModuleClass,
                "releaseSQLiteConnection",
                "(I)V"
        );

        SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(env->CallLongMethod(bridge, getConnectionMethod, jTag));

        sqlite3* db = connection->db;

        auto stmt = getStmt(rt, reinterpret_cast<sqlite3*>(db), sql.utf8(rt), arguments);

        std::vector<jsi::Value> records = {};

        while (true) {
            if (getNextRowOrTrue(rt, stmt)) {
                break;
            }

            jsi::Object record = resultDictionary(rt, stmt);

            records.push_back(std::move(record));
        }

        finalizeStmt(stmt);
        env->CallVoidMethod(bridge, releaseConnectionMethod, jTag);

        return arrayFromStd(rt, records);
    }

    jsi::Value query(jobject bridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &table, const jsi::String &query) {
        JNIEnv *env = getEnv();

        // Convert the jsi::Value arguments to std::string
        jint jTag = static_cast<jint>(tag.asNumber());

        auto tableStr = table.utf8(rt);
        auto queryStr = query.utf8(rt);

        jclass myNativeModuleClass = env->GetObjectClass(bridge);

        jmethodID getConnectionMethod = env->GetMethodID(
                myNativeModuleClass,
                "getSQLiteConnection",
                "(I)J"
        );

        jmethodID releaseConnectionMethod = env->GetMethodID(
                myNativeModuleClass,
                "releaseSQLiteConnection",
                "(I)V"
        );

        SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(env->CallLongMethod(bridge, getConnectionMethod, jTag));

        sqlite3* db = connection->db;

        auto stmt = getStmt(rt, db, query.utf8(rt), jsi::Array(rt, 0));

        std::vector<jsi::Value> records = {};

        while (true) {
            if (getNextRowOrTrue(rt, stmt)) {
                break;
            }

            assert(std::string(sqlite3_column_name(stmt, 0)) == "id");

            const char *id = (const char *)sqlite3_column_text(stmt, 0);

            if (!id) {
                throw jsi::JSError(rt, "Failed to get ID of a record");
            }

            jstring jId = env->NewStringUTF(id);
            jstring jTable = env->NewStringUTF(tableStr.c_str());

            jmethodID isCachedMethod = env->GetMethodID(
                    myNativeModuleClass,
                    "isCached",
                    "(ILjava/lang/String;Ljava/lang/String;)Z");

            bool isCached = env->CallBooleanMethod(bridge, isCachedMethod, jTag, jTable, jId);

            if (isCached) {
                jsi::String jsiId = jsi::String::createFromAscii(rt, id);
                records.push_back(std::move(jsiId));
            } else {
                jmethodID markAsCachedMethod = env->GetMethodID(
                        myNativeModuleClass,
                        "markAsCached",
                        "(ILjava/lang/String;Ljava/lang/String;)V");

                env->CallVoidMethod(bridge, markAsCachedMethod, jTag, jTable, jId);
                jsi::Object record = resultDictionary(rt, stmt);
                records.push_back(std::move(record));
            }

            env->DeleteLocalRef(jId);
            env->DeleteLocalRef(jTable);
        }

        finalizeStmt(stmt);
        env->CallVoidMethod(bridge, releaseConnectionMethod, jTag);

        return arrayFromStd(rt, records);
    }

} // namespace watermelondb 