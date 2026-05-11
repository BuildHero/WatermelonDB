#import "SliceImportDatabaseAdapter.h"

#include "SliceImportEngine.h"
#include "SqliteInsertHelper.h"

#import <sqlite3.h>

#import <React/RCTEventEmitter.h>

#if __has_include("WatermelonDB-Swift.h")
#import <WatermelonDB-Swift.h>
#else
#import "WatermelonDB-Swift.h"
#endif

#include <string>
#include <atomic>

using watermelondb::DatabaseInterface;
using watermelondb::FieldValue;

namespace {
static void *kDBQueueKey = &kDBQueueKey;
static std::atomic<uint64_t> sliceImportSeq{0};

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
}

class IOSDatabaseInterface final : public DatabaseInterface {
public:
    IOSDatabaseInterface(DatabaseBridge *db, NSNumber *connectionTag)
        : db_(db)
        , connectionTag_(connectionTag)
        , methodQueue_(db ? db.methodQueue : nullptr)
        , transactionStarted_(false)
        , writerSemaphore_(nil)
        , cachedDB_(nullptr)
        , holderName_("")
        , txnStartAbsTime_(0.0) {
        if (methodQueue_) {
            dispatch_queue_set_specific(methodQueue_, kDBQueueKey, kDBQueueKey, nullptr);
        }
    }

    ~IOSDatabaseInterface() override {
        if (transactionStarted_) {
            std::string error;
            rollbackTransaction();
        } else {
            finalizeStatements();
        }
    }

    bool beginTransaction(std::string &errorMessage) override {
        if (!db_) {
            errorMessage = "DatabaseBridge deallocated";
            return false;
        }
        if (transactionStarted_) {
            errorMessage = "Transaction already started";
            return false;
        }

        // Build a unique holder tag so we can attribute BUSY collisions.
        uint64_t seq = ++sliceImportSeq;
        char holderBuf[64];
        snprintf(holderBuf, sizeof(holderBuf), "slice-import:%lld:#%llu",
                 (long long)[connectionTag_ longLongValue], (unsigned long long)seq);
        holderName_ = holderBuf;

        // Acquire the writer transaction semaphore — blocks until JS writes finish
        dispatch_semaphore_t sem = [db_ getWriterTransactionSemaphoreWithConnectionTag:connectionTag_];
        if (!sem) {
            errorMessage = "Could not get writer transaction semaphore";
            return false;
        }
        NSString *prevHolder = [db_ currentWriterHolderWithConnectionTag:connectionTag_];
        NSTimeInterval waitStart = [NSDate timeIntervalSinceReferenceDate];
        dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
        double waitMs = ([NSDate timeIntervalSinceReferenceDate] - waitStart) * 1000.0;
        if (waitMs > 50.0) {
            NSLog(@"[wmdb-lock] %s waited %.0fms for sem (prev holder: %@)",
                  holderName_.c_str(), waitMs, prevHolder);
        }
        writerSemaphore_ = sem;
        [db_ setWriterHolderWithConnectionTag:connectionTag_
                                         name:[NSString stringWithUTF8String:holderName_.c_str()]];

        // Get raw sqlite3* directly (bypasses methodQueue — semaphore provides serialization)
        sqlite3 *db = (sqlite3 *)[db_ getRawConnectionWithConnectionTag:connectionTag_];
        if (!db) {
            [db_ clearWriterHolderWithConnectionTag:connectionTag_];
            dispatch_semaphore_signal(writerSemaphore_);
            writerSemaphore_ = nil;
            errorMessage = "Lost database connection";
            return false;
        }
        cachedDB_ = db;

        std::string ignored;
        execSQL(db, "PRAGMA busy_timeout=5000;", ignored);
        execSQL(db, "PRAGMA journal_mode=WAL;", ignored);
        execSQL(db, "PRAGMA synchronous=NORMAL;", ignored);
        execSQL(db, "PRAGMA temp_store=MEMORY;", ignored);
        execSQL(db, "PRAGMA cache_size=-20000;", ignored);
        execSQL(db, "PRAGMA wal_autocheckpoint=10000;", ignored);
        txnStartAbsTime_ = [NSDate timeIntervalSinceReferenceDate];
        if (!execSQL(db, "BEGIN IMMEDIATE;", errorMessage)) {
            NSLog(@"[wmdb-lock] %s BEGIN IMMEDIATE failed: %s (holder reported: %@)",
                  holderName_.c_str(), errorMessage.c_str(),
                  [db_ currentWriterHolderWithConnectionTag:connectionTag_]);
            [db_ clearWriterHolderWithConnectionTag:connectionTag_];
            dispatch_semaphore_signal(writerSemaphore_);
            writerSemaphore_ = nil;
            cachedDB_ = nullptr;
            return false;
        }
        transactionStarted_ = true;
        return true;
    }

    bool commitTransaction(std::string &errorMessage) override {
        if (!transactionStarted_) {
            errorMessage = "No transaction to commit";
            return false;
        }
        sqlite3 *db = cachedDB_;
        if (!db) {
            errorMessage = "Lost cached database connection";
            return false;
        }
        if (!execSQL(db, "COMMIT;", errorMessage)) {
            NSLog(@"[wmdb-lock] %s COMMIT failed: %s", holderName_.c_str(), errorMessage.c_str());
            rollbackTransactionOnDB(db);
            return false;
        }
        transactionStarted_ = false;
        double heldMs = ([NSDate timeIntervalSinceReferenceDate] - txnStartAbsTime_) * 1000.0;
        NSLog(@"[wmdb-lock] %s COMMIT ok (txn held %.0fms)", holderName_.c_str(), heldMs);

        // WAL checkpoint for predictable completion
        int logFrames = 0;
        int ckptFrames = 0;
        sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_TRUNCATE, &logFrames, &ckptFrames);

        finalizeStatementsOnDB(db);

        std::string ignored;
        execSQL(db, "PRAGMA synchronous=NORMAL;", ignored);
        execSQL(db, "PRAGMA wal_autocheckpoint=1000;", ignored);

        cachedDB_ = nullptr;
        [db_ clearWriterHolderWithConnectionTag:connectionTag_];
        if (writerSemaphore_) {
            dispatch_semaphore_signal(writerSemaphore_);
            writerSemaphore_ = nil;
        }

        return true;
    }

    void rollbackTransaction() override {
        sqlite3 *db = cachedDB_;
        if (db) {
            double heldMs = ([NSDate timeIntervalSinceReferenceDate] - txnStartAbsTime_) * 1000.0;
            NSLog(@"[wmdb-lock] %s ROLLBACK (txn held %.0fms)", holderName_.c_str(), heldMs);
            rollbackTransactionOnDB(db);
        }
        cachedDB_ = nullptr;
        [db_ clearWriterHolderWithConnectionTag:connectionTag_];
        if (writerSemaphore_) {
            dispatch_semaphore_signal(writerSemaphore_);
            writerSemaphore_ = nil;
        }
    }

    bool insertRows(const std::string &tableName,
                    const std::vector<std::string> &columns,
                    const std::vector<std::vector<FieldValue>> &rows,
                    std::string &errorMessage) override {
        if (rows.empty()) return true;
        sqlite3 *db = cachedDB_;
        if (!db) {
            errorMessage = "No cached database connection";
            return false;
        }
        return insertHelper_.insertRowsMulti(db, tableName, columns, rows, errorMessage);
    }

    bool insertBatch(const watermelondb::BatchData &batch,
                     std::string &errorMessage) override {
        if (batch.totalRows == 0) return true;
        sqlite3 *db = cachedDB_;
        if (!db) {
            errorMessage = "No cached database connection";
            return false;
        }
        return insertHelper_.insertBatch(db, batch, errorMessage);
    }

    bool createSavepoint(std::string &errorMessage) override {
        sqlite3 *db = cachedDB_;
        if (!db) {
            errorMessage = "No cached database connection";
            return false;
        }
        return execSQL(db, "SAVEPOINT sp;", errorMessage);
    }

    bool releaseSavepoint(std::string &errorMessage) override {
        sqlite3 *db = cachedDB_;
        if (!db) {
            errorMessage = "No cached database connection";
            return false;
        }
        return execSQL(db, "RELEASE SAVEPOINT sp;", errorMessage);
    }

private:
    __weak DatabaseBridge *db_;
    NSNumber *connectionTag_;
    dispatch_queue_t methodQueue_;
    watermelondb::SqliteInsertHelper insertHelper_;
    bool transactionStarted_;
    dispatch_semaphore_t writerSemaphore_;
    sqlite3 *cachedDB_;
    std::string holderName_;
    NSTimeInterval txnStartAbsTime_;

    void finalizeStatements() {
        if (cachedDB_) {
            finalizeStatementsOnDB(cachedDB_);
            return;
        }
        if (!methodQueue_) return;
        void (^block)(void) = ^{
            sqlite3 *db = (sqlite3 *)[db_ getRawConnectionWithConnectionTag:connectionTag_];
            if (db) {
                finalizeStatementsOnDB(db);
            }
        };
        if (dispatch_get_specific(kDBQueueKey)) {
            block();
        } else {
            dispatch_sync(methodQueue_, block);
        }
    }

    void finalizeStatementsOnDB(sqlite3 *db) {
        (void)db;
        insertHelper_.finalizeStatements();
    }

    void rollbackTransactionOnDB(sqlite3 *db) {
        std::string ignored;
        execSQL(db, "ROLLBACK TO SAVEPOINT sp;", ignored);
        execSQL(db, "RELEASE SAVEPOINT sp;", ignored);
        execSQL(db, "ROLLBACK;", ignored);

        finalizeStatementsOnDB(db);
        transactionStarted_ = false;

        execSQL(db, "PRAGMA synchronous=NORMAL;", ignored);
        execSQL(db, "PRAGMA wal_autocheckpoint=1000;", ignored);
    }
};

std::shared_ptr<DatabaseInterface> createIOSDatabaseInterface(DatabaseBridge *db, NSNumber *connectionTag) {
    return std::make_shared<IOSDatabaseInterface>(db, connectionTag);
}
