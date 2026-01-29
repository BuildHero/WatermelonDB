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

using watermelondb::DatabaseInterface;
using watermelondb::FieldValue;

namespace {
static void *kDBQueueKey = &kDBQueueKey;

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
        , transactionStarted_(false) {
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
        return runOnDBQueue([this](sqlite3 *db, std::string &error) {
            if (transactionStarted_) {
                error = "Transaction already started";
                return false;
            }
            std::string ignored;
            execSQL(db, "PRAGMA journal_mode=WAL;", ignored);
            execSQL(db, "PRAGMA synchronous=NORMAL;", ignored);
            execSQL(db, "PRAGMA temp_store=MEMORY;", ignored);
            execSQL(db, "PRAGMA cache_size=-20000;", ignored);
            execSQL(db, "PRAGMA wal_autocheckpoint=10000;", ignored);
            if (!execSQL(db, "BEGIN IMMEDIATE;", error)) {
                return false;
            }
            transactionStarted_ = true;
            return true;
        }, errorMessage);
    }
    
    bool commitTransaction(std::string &errorMessage) override {
        return runOnDBQueue([this](sqlite3 *db, std::string &error) {
            if (!transactionStarted_) {
                error = "No transaction to commit";
                return false;
            }
            if (!execSQL(db, "COMMIT;", error)) {
                rollbackTransactionOnDB(db);
                return false;
            }
            transactionStarted_ = false;
            
            // WAL checkpoint for predictable completion
            int logFrames = 0;
            int ckptFrames = 0;
            sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_TRUNCATE, &logFrames, &ckptFrames);
            
            finalizeStatementsOnDB(db);
            
            std::string ignored;
            execSQL(db, "PRAGMA synchronous=NORMAL;", ignored);
            execSQL(db, "PRAGMA wal_autocheckpoint=1000;", ignored);
            
            return true;
        }, errorMessage);
    }
    
    void rollbackTransaction() override {
        std::string ignored;
        runOnDBQueue([this](sqlite3 *db, std::string &error) {
            rollbackTransactionOnDB(db);
            return true;
        }, ignored);
    }
    
    bool insertRows(const std::string &tableName,
                    const std::vector<std::string> &columns,
                    const std::vector<std::vector<FieldValue>> &rows,
                    std::string &errorMessage) override {
        if (rows.empty()) {
            return true;
        }
        return runOnDBQueue([this, &tableName, &columns, &rows](sqlite3 *db, std::string &error) {
            return insertHelper_.insertRowsMulti(db, tableName, columns, rows, error);
        }, errorMessage);
    }
    
    bool insertBatch(const watermelondb::BatchData &batch,
                     std::string &errorMessage) override {
        if (batch.totalRows == 0) {
            return true;
        }
        
        return runOnDBQueue([this, &batch](sqlite3 *db, std::string &error) {
            return insertHelper_.insertBatch(db, batch, error);
        }, errorMessage);
    }
    
    bool createSavepoint(std::string &errorMessage) override {
        return runOnDBQueue([](sqlite3 *db, std::string &error) {
            return execSQL(db, "SAVEPOINT sp;", error);
        }, errorMessage);
    }
    
    bool releaseSavepoint(std::string &errorMessage) override {
        return runOnDBQueue([](sqlite3 *db, std::string &error) {
            return execSQL(db, "RELEASE SAVEPOINT sp;", error);
        }, errorMessage);
    }
    
private:
    __weak DatabaseBridge *db_;
    NSNumber *connectionTag_;
    dispatch_queue_t methodQueue_;
    watermelondb::SqliteInsertHelper insertHelper_;
    bool transactionStarted_;
    
    bool runOnDBQueue(const std::function<bool(sqlite3 *, std::string &)> &work,
                      std::string &errorMessage) {
        if (!db_ || !methodQueue_) {
            errorMessage = "DatabaseBridge deallocated";
            return false;
        }
        __block bool ok = false;
        __block std::string error;
        void (^block)(void) = ^{
            sqlite3 *db = (sqlite3 *)[db_ getRawConnectionWithConnectionTag:connectionTag_];
            if (!db) {
                error = "Lost database connection";
                ok = false;
                return;
            }
            ok = work(db, error);
        };
        if (dispatch_get_specific(kDBQueueKey)) {
            block();
        } else {
            dispatch_sync(methodQueue_, block);
        }
        if (!error.empty()) {
            errorMessage = error;
        }
        return ok;
    }
    
    void finalizeStatements() {
        if (!methodQueue_) {
            return;
        }
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
