#include "JSIAndroidUtils.h"
#include "../../../../shared/DatabaseUtils.h"
#include <string>
#include <fbjni/fbjni.h>
#include <sqlite3.h>
#include <android/log.h>
#include <mutex>
#include <condition_variable>
#include <chrono>

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

template <typename T>
class LocalRef {
public:
    LocalRef(JNIEnv* env, T obj) : env_(env), obj_(obj) {}
    ~LocalRef() {
        if (obj_) {
            env_->DeleteLocalRef(obj_);
        }
    }
    T get() const { return obj_; }
private:
    JNIEnv* env_;
    T obj_;
};

namespace watermelondb {
    static JavaVM* gJvm = nullptr;
    static std::mutex gJvmMutex;
    static std::condition_variable gJvmCv;
    static bool gJvmReady = false;
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
        if (gJvm) {
            JNIEnv* env = nullptr;
            jint status = gJvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);
            if (status == JNI_EDETACHED) {
                if (gJvm->AttachCurrentThread(&env, nullptr) != JNI_OK) {
                    return nullptr;
                }
            } else if (status != JNI_OK) {
                return nullptr;
            }
            return env;
        }
        return facebook::jni::Environment::current();
    }

    JNIEnv* attachCurrentThread() {
        if (!gJvm) {
            return nullptr;
        }
        JNIEnv* env = nullptr;
        jint status = gJvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);
        if (status == JNI_EDETACHED) {
            if (gJvm->AttachCurrentThread(&env, nullptr) != JNI_OK) {
                return nullptr;
            }
        } else if (status != JNI_OK) {
            return nullptr;
        }
        return env;
    }

    bool waitForJvm(int timeoutMs) {
        std::unique_lock<std::mutex> lock(gJvmMutex);
        if (gJvmReady) {
            return true;
        }
        bool ok = gJvmCv.wait_for(lock, std::chrono::milliseconds(timeoutMs), []() { return gJvmReady; });
        return ok;
    }

    void configureJNI(JNIEnv *env) {
        if (!env) {
            return;
        }
        JavaVM* vm = nullptr;
        if (env->GetJavaVM(&vm) == JNI_OK) {
            {
                std::lock_guard<std::mutex> lock(gJvmMutex);
                gJvm = vm;
                gJvmReady = true;
            }
            gJvmCv.notify_all();
        }
    }

    jsi::Value execSqlQuery(jobject bridge, jsi::Runtime &rt, const jsi::Value &tag, const jsi::String &sql, const jsi::Array &arguments) {
        JNIEnv *env = getEnv();
        if (!env) {
            throw jsi::JSError(rt, "JNI env not available");
        }
        jint jTag = static_cast<jint>(tag.asNumber());

        std::string queryStr = sql.utf8(rt);

        LocalRef<jclass> myNativeModuleClass(env, env->GetObjectClass(bridge));

        jmethodID getConnectionMethod = env->GetMethodID(
                myNativeModuleClass.get(),
                "getSQLiteConnection",
                "(I)J"
        );

        jmethodID releaseConnectionMethod = env->GetMethodID(
                myNativeModuleClass.get(),
                "releaseSQLiteConnection",
                "(I)V"
        );

        SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(env->CallLongMethod(bridge, getConnectionMethod, jTag));
        
        // Check if a Java exception occurred (e.g., "No driver with tag X available")
        if (env->ExceptionCheck()) {
            LocalRef<jthrowable> exception(env, env->ExceptionOccurred());
            env->ExceptionClear();
            
            // Get the exception message
            LocalRef<jclass> exceptionClass(env, env->GetObjectClass(exception.get()));
            jmethodID getMessageMethod = env->GetMethodID(exceptionClass.get(), "getMessage", "()Ljava/lang/String;");
            LocalRef<jstring> messageObj(env, (jstring)env->CallObjectMethod(exception.get(), getMessageMethod));
            
            std::string message = "Database connection error for tag " + std::to_string(jTag);
            if (messageObj.get()) {
                const char* messageChars = env->GetStringUTFChars(messageObj.get(), nullptr);
                message = std::string(messageChars);
                env->ReleaseStringUTFChars(messageObj.get(), messageChars);
            }

            throw jsi::JSError(rt, message);
        }
        
        if (!connection) {
            throw jsi::JSError(rt, "Failed to get SQLite connection - connection is null");
        }
        
        if (!connection->db) {
            env->CallVoidMethod(bridge, releaseConnectionMethod, jTag);
            throw jsi::JSError(rt, "Failed to get SQLite connection - database handle is null");
        }

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
        if (!env) {
            throw jsi::JSError(rt, "JNI env not available");
        }

        // Convert the jsi::Value arguments to std::string
        jint jTag = static_cast<jint>(tag.asNumber());

        auto tableStr = table.utf8(rt);
        auto queryStr = query.utf8(rt);

        LocalRef<jclass> myNativeModuleClass(env, env->GetObjectClass(bridge));

        jmethodID getConnectionMethod = env->GetMethodID(
                myNativeModuleClass.get(),
                "getSQLiteConnection",
                "(I)J"
        );

        jmethodID releaseConnectionMethod = env->GetMethodID(
                myNativeModuleClass.get(),
                "releaseSQLiteConnection",
                "(I)V"
        );

        SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(env->CallLongMethod(bridge, getConnectionMethod, jTag));
        
        // Check if a Java exception occurred (e.g., "No driver with tag X available")
        if (env->ExceptionCheck()) {
            LocalRef<jthrowable> exception(env, env->ExceptionOccurred());
            env->ExceptionClear();
            
            // Get the exception message
            LocalRef<jclass> exceptionClass(env, env->GetObjectClass(exception.get()));
            jmethodID getMessageMethod = env->GetMethodID(exceptionClass.get(), "getMessage", "()Ljava/lang/String;");
            LocalRef<jstring> messageObj(env, (jstring)env->CallObjectMethod(exception.get(), getMessageMethod));
            
            std::string message = "Database connection error for tag " + std::to_string(jTag);
            if (messageObj.get()) {
                const char* messageChars = env->GetStringUTFChars(messageObj.get(), nullptr);
                message = std::string(messageChars);
                env->ReleaseStringUTFChars(messageObj.get(), messageChars);
            }

            throw jsi::JSError(rt, message);
        }
        
        if (!connection) {
            throw jsi::JSError(rt, "Failed to get SQLite connection - connection is null");
        }
        
        if (!connection->db) {
            env->CallVoidMethod(bridge, releaseConnectionMethod, jTag);
            throw jsi::JSError(rt, "Failed to get SQLite connection - database handle is null");
        }

        sqlite3* db = connection->db;

        auto stmt = getStmt(rt, db, query.utf8(rt), jsi::Array(rt, 0));

        std::vector<jsi::Value> records = {};

        while (true) {
            if (getNextRowOrTrue(rt, stmt)) {
                break;
            }

            // Validate first column is 'id' before proceeding
            const char* firstColumnName = sqlite3_column_name(stmt, 0);
            if (!firstColumnName || std::string(firstColumnName) != "id") {
                finalizeStmt(stmt);
                env->CallVoidMethod(bridge, releaseConnectionMethod, jTag);
                throw jsi::JSError(rt, "Query result does not have 'id' as first column");
            }

            const char *id = (const char *)sqlite3_column_text(stmt, 0);

            if (!id) {
                throw jsi::JSError(rt, "Failed to get ID of a record");
            }

            jstring jId = env->NewStringUTF(id);
            jstring jTable = env->NewStringUTF(tableStr.c_str());

            jmethodID isCachedMethod = env->GetMethodID(
                    myNativeModuleClass.get(),
                    "isCached",
                    "(ILjava/lang/String;Ljava/lang/String;)Z");

            bool isCached = env->CallBooleanMethod(bridge, isCachedMethod, jTag, jTable, jId);

            if (isCached) {
                jsi::String jsiId = jsi::String::createFromAscii(rt, id);
                records.push_back(std::move(jsiId));
            } else {
                jmethodID markAsCachedMethod = env->GetMethodID(
                        myNativeModuleClass.get(),
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
