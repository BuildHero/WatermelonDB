#include "SliceImportDatabaseAdapterAndroid.h"
#include "JSIAndroidUtils.h"

#include "SliceImportEngine.h"
#include "SqliteInsertHelper.h"
#include "SlicePlatformAndroidQueue.h"

#include <sqlite3.h>
#include <jni.h>
#include <string>
#include <thread>
#include <future>

using watermelondb::DatabaseInterface;
using watermelondb::FieldValue;

namespace watermelondb {
namespace platform {
void runOnWorkQueue(const std::function<void()>& work);
} // namespace platform
} // namespace watermelondb

namespace {
static bool execSQL(sqlite3 *db, const char *sql, std::string &errorMessage) {
    char *errMsg = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        if (errMsg) {
            errorMessage = errMsg;
            sqlite3_free(errMsg);
        } else {
            errorMessage = "SQLite error";
        }
        return false;
    }
    return true;
}

struct SQLiteConnection {
    sqlite3* const db;
    const int openFlags;
    char* path;
    char* label;
    volatile bool canceled;
};

static bool runOnAndroidWorkQueueSync(const std::function<void()>& work, std::string* errorMessage) {
    if (!watermelondb::waitForJvm(5000)) {
        if (errorMessage) {
            *errorMessage = "JVM not ready for work queue";
        }
        return false;
    }
    if (watermelondb::android::isOnWorkQueue()) {
        work();
        return true;
    }
    std::promise<void> done;
    auto future = done.get_future();
    watermelondb::android::runOnWorkQueue([&work, &done]() {
        work();
        done.set_value();
    });
    future.wait();
    return true;
}

class AndroidDatabaseInterface final : public DatabaseInterface {
public:
    AndroidDatabaseInterface(jobject bridge, jint connectionTag)
        : bridgeGlobal_(nullptr)
        , connectionTag_(connectionTag)
        , transactionStarted_(false)
        , db_(nullptr) {
        JNIEnv* env = watermelondb::getEnv();
        if (env && bridge) {
            bridgeGlobal_ = env->NewGlobalRef(bridge);
        }
    }

    ~AndroidDatabaseInterface() override {
        if (transactionStarted_) {
            rollbackTransaction();
        } else {
            finalizeStatements();
            releaseConnection();
        }
        if (bridgeGlobal_) {
            watermelondb::getEnv()->DeleteGlobalRef(bridgeGlobal_);
            bridgeGlobal_ = nullptr;
        }
    }

    bool beginTransaction(std::string &errorMessage) override {
        bool ok = false;
        if (!runOnAndroidWorkQueueSync([&]() {
            if (transactionStarted_) {
                errorMessage = "Transaction already started";
                ok = false;
                return;
            }
            if (!ensureConnection(errorMessage)) {
                ok = false;
                return;
            }
            std::string ignored;
            execSQL(db_, "PRAGMA journal_mode=WAL;", ignored);
            execSQL(db_, "PRAGMA synchronous=NORMAL;", ignored);
            execSQL(db_, "PRAGMA temp_store=MEMORY;", ignored);
            execSQL(db_, "PRAGMA cache_size=-20000;", ignored);
            execSQL(db_, "PRAGMA wal_autocheckpoint=10000;", ignored);
            if (!execSQL(db_, "BEGIN IMMEDIATE;", errorMessage)) {
                releaseConnection();
                ok = false;
                return;
            }
            transactionStarted_ = true;
            ownerThread_ = std::this_thread::get_id();
            ok = true;
        }, &errorMessage)) {
            return false;
        }
        return ok;
    }

    bool commitTransaction(std::string &errorMessage) override {
        bool ok = false;
        if (!runOnAndroidWorkQueueSync([&]() {
            if (!transactionStarted_) {
                errorMessage = "No transaction to commit";
                ok = false;
                return;
            }
            if (!execSQL(db_, "COMMIT;", errorMessage)) {
                rollbackTransactionOnDB();
                releaseConnection();
                ok = false;
                return;
            }
            transactionStarted_ = false;

            int logFrames = 0;
            int ckptFrames = 0;
            sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &logFrames, &ckptFrames);

            finalizeStatementsOnDB();

            std::string ignored;
            execSQL(db_, "PRAGMA synchronous=NORMAL;", ignored);
            execSQL(db_, "PRAGMA wal_autocheckpoint=1000;", ignored);

            releaseConnection();
            ok = true;
        }, &errorMessage)) {
            return false;
        }
        return ok;
    }

    void rollbackTransaction() override {
        runOnAndroidWorkQueueSync([&]() {
            if (!db_) {
                return;
            }
            rollbackTransactionOnDB();
            releaseConnection();
        }, nullptr);
    }

    bool insertRows(const std::string &tableName,
                    const std::vector<std::string> &columns,
                    const std::vector<std::vector<FieldValue>> &rows,
                    std::string &errorMessage) override {
        if (rows.empty()) {
            return true;
        }
        bool ok = false;
        if (!runOnAndroidWorkQueueSync([&]() {
            if (!db_) {
                errorMessage = "No active database connection";
                ok = false;
                return;
            }
            ok = insertHelper_.insertRowsMulti(db_, tableName, columns, rows, errorMessage);
        }, &errorMessage)) {
            return false;
        }
        return ok;
    }

    bool insertBatch(const watermelondb::BatchData &batch,
                     std::string &errorMessage) override {
        if (batch.totalRows == 0) {
            return true;
        }
        bool ok = false;
        if (!runOnAndroidWorkQueueSync([&]() {
            if (!db_) {
                errorMessage = "No active database connection";
                ok = false;
                return;
            }
            ok = insertHelper_.insertBatch(db_, batch, errorMessage);
        }, &errorMessage)) {
            return false;
        }
        return ok;
    }

    bool createSavepoint(std::string &errorMessage) override {
        bool ok = false;
        if (!runOnAndroidWorkQueueSync([&]() {
            if (!db_) {
                errorMessage = "No active database connection";
                ok = false;
                return;
            }
            ok = execSQL(db_, "SAVEPOINT sp;", errorMessage);
        }, &errorMessage)) {
            return false;
        }
        return ok;
    }

    bool releaseSavepoint(std::string &errorMessage) override {
        bool ok = false;
        if (!runOnAndroidWorkQueueSync([&]() {
            if (!db_) {
                errorMessage = "No active database connection";
                ok = false;
                return;
            }
            ok = execSQL(db_, "RELEASE SAVEPOINT sp;", errorMessage);
        }, &errorMessage)) {
            return false;
        }
        return ok;
    }

private:
    jobject bridgeGlobal_;
    jint connectionTag_;
    bool transactionStarted_;
    sqlite3* db_;
    std::thread::id ownerThread_;
    watermelondb::SqliteInsertHelper insertHelper_;

    bool ensureConnection(std::string &errorMessage) {
        if (db_) {
            return true;
        }
        JNIEnv* env = watermelondb::getEnv();
        if (!env || !bridgeGlobal_) {
            errorMessage = "DatabaseBridge not available";
            return false;
        }
        jclass cls = env->GetObjectClass(bridgeGlobal_);
        if (!cls) {
            env->ExceptionClear();
            errorMessage = "DatabaseBridge class not found";
            return false;
        }
        jmethodID getConn = env->GetMethodID(cls, "getSQLiteConnection", "(I)J");
        if (!getConn) {
            env->ExceptionClear();
            env->DeleteLocalRef(cls);
            errorMessage = "getSQLiteConnection not found";
            return false;
        }
        jlong ptr = env->CallLongMethod(bridgeGlobal_, getConn, connectionTag_);
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
            env->DeleteLocalRef(cls);
            errorMessage = "Failed to get SQLite connection";
            return false;
        }
        env->DeleteLocalRef(cls);
        if (!ptr) {
            errorMessage = "SQLite connection pointer is null";
            return false;
        }
        auto connection = reinterpret_cast<SQLiteConnection*>(ptr);
        if (!connection || !connection->db) {
            errorMessage = "SQLite connection invalid";
            return false;
        }
        db_ = connection->db;
        return true;
    }

    void releaseConnection() {
        if (!db_) {
            return;
        }
        JNIEnv* env = watermelondb::getEnv();
        if (!env || !bridgeGlobal_) {
            db_ = nullptr;
            return;
        }
        jclass cls = env->GetObjectClass(bridgeGlobal_);
        if (!cls) {
            env->ExceptionClear();
            db_ = nullptr;
            return;
        }
        jmethodID releaseConn = env->GetMethodID(cls, "releaseSQLiteConnection", "(I)V");
        if (releaseConn) {
            env->CallVoidMethod(bridgeGlobal_, releaseConn, connectionTag_);
        } else {
            env->ExceptionClear();
        }
        env->DeleteLocalRef(cls);
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
        db_ = nullptr;
    }

    void finalizeStatements() {
        insertHelper_.finalizeStatements();
    }

    void finalizeStatementsOnDB() {
        insertHelper_.finalizeStatements();
    }

    void rollbackTransactionOnDB() {
        std::string ignored;
        execSQL(db_, "ROLLBACK TO SAVEPOINT sp;", ignored);
        execSQL(db_, "RELEASE SAVEPOINT sp;", ignored);
        execSQL(db_, "ROLLBACK;", ignored);

        finalizeStatementsOnDB();
        transactionStarted_ = false;

        execSQL(db_, "PRAGMA synchronous=NORMAL;", ignored);
        execSQL(db_, "PRAGMA wal_autocheckpoint=1000;", ignored);
    }
};
} // namespace

std::shared_ptr<DatabaseInterface> createAndroidDatabaseInterface(jobject bridge, jint connectionTag) {
    return std::make_shared<AndroidDatabaseInterface>(bridge, connectionTag);
}
