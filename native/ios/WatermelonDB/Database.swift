import Foundation
import SQLite3

/// Runtime gate for writer-lock diagnostic logging. Default `true` in DEBUG,
/// `false` in release. Toggleable from native code via `WMDBLockLog.isEnabled`
/// (Obj-C: `WMDBLockLog.isEnabled = YES`). When disabled, all `[wmdb-lock]`
/// log sites short-circuit at the call site before any string formatting.
@objc(WMDBLockLog)
public final class WMDBLockLog: NSObject {
    private static var _enabled: Bool = {
        #if DEBUG
        return true
        #else
        return false
        #endif
    }()

    @objc
    public class var isEnabled: Bool {
        get { return _enabled }
        set { _enabled = newValue }
    }
}

@inline(__always)
internal func wmdbLockLog(_ message: @autoclosure () -> String) {
    if WMDBLockLog.isEnabled {
        consoleLog(message())
    }
}

public class Database {
    public typealias SQL = String
    public typealias TableName = String
    public typealias QueryArgs = [Any]
    
    private let writer: FMDatabase
    private let reader: FMDatabase
    private let path: String
    private let transactionLock = NSLock()
    private var _transactionDepth = 0
    private var transactionDepth: Int {
        get {
            transactionLock.lock()
            defer { transactionLock.unlock() }
            return _transactionDepth
        }
        set {
            transactionLock.lock()
            defer { transactionLock.unlock() }
            _transactionDepth = newValue
        }
    }
    
    private var updateHookCallback: ((UnsafeMutableRawPointer?, Int32, UnsafePointer<Int8>?, UnsafePointer<Int8>?, Int64) -> Void)?

    /// Semaphore that serializes all write transactions across JS batch, SyncApply,
    /// and SliceImport. Acquired before BEGIN, released after COMMIT/ROLLBACK.
    public let writerTransactionSemaphore = DispatchSemaphore(value: 1)

    // MARK: - Writer-lock holder tracking (diagnostics)
    // Tracks who currently holds the writer (or has it BEGIN IMMEDIATE'd) so
    // that BUSY errors on the non-transactional writer path can attribute the
    // collision. Acquirers (Database.inTransaction, slice import, sync apply)
    // set this on acquire and clear on release.
    private let holderLock = NSLock()
    private var _currentHolder: String = "(none)"
    private var _holderAcquiredAt: Date?

    public func setWriterHolder(_ name: String) {
        holderLock.lock()
        let prev = _currentHolder
        _currentHolder = name
        _holderAcquiredAt = Date()
        holderLock.unlock()
        wmdbLockLog("[wmdb-lock] holder=\(name) acquired (prev=\(prev))")
    }

    public func clearWriterHolder() {
        holderLock.lock()
        let name = _currentHolder
        let durationMs = _holderAcquiredAt.map { Date().timeIntervalSince($0) * 1000 } ?? 0
        _currentHolder = "(none)"
        _holderAcquiredAt = nil
        holderLock.unlock()
        wmdbLockLog(String(format: "[wmdb-lock] holder=%@ released (held %.0fms)", name, durationMs))
    }

    public func currentHolderInfo() -> String {
        holderLock.lock()
        defer { holderLock.unlock() }
        if let acquiredAt = _holderAcquiredAt {
            let elapsed = Date().timeIntervalSince(acquiredAt) * 1000
            return String(format: "%@ (held %.0fms)", _currentHolder, elapsed)
        }
        return _currentHolder
    }

    private func isBusyError(_ message: String) -> Bool {
        let lower = message.lowercased()
        return lower.contains("locked") || lower.contains("busy")
    }

    init(path: String) {
        self.path = path
        writer = FMDatabase(path: path)
        if Database.isInMemory(path: path) {
            reader = writer
        } else {
            reader = FMDatabase(path: path)
        }
        open()
    }

    deinit {
        // CRITICAL: Clean up SQLite update hook before deallocation
        // If we don't do this, the hook callback can fire on a dangling pointer causing crashes
        disableUpdateHook()
    }

    func close() {
        disableUpdateHook()
        writer.close()
        if reader !== writer {
            reader.close()
        }
    }
    
    private func open() {
        guard writer.open() else {
            fatalError("Failed to open the database. \(writer.lastErrorMessage())")
        }
        
        if reader !== writer {
            guard reader.open() else {
                fatalError("Failed to open the database. \(reader.lastErrorMessage())")
            }
        }
        
        do {
            try setWalMode(on: writer)
            if reader !== writer {
                try setWalMode(on: reader)
                try setQueryOnly(on: reader)
            }
        } catch {
            fatalError("Failed to configure database connections \(error)")
        }
        
        consoleLog("Opened database at: \(path)")
    }
    
    func inTransaction(_ executeBlock: () throws -> Void) throws {
        // Skip wait-time accounting entirely when diagnostics are off.
        let diagnosticsOn = WMDBLockLog.isEnabled
        let holderBeforeWait = diagnosticsOn ? currentHolderInfo() : ""
        let waitStart = diagnosticsOn ? Date() : nil
        writerTransactionSemaphore.wait()
        if let waitStart = waitStart {
            let waitMs = Date().timeIntervalSince(waitStart) * 1000
            if waitMs > 50 {
                wmdbLockLog(String(format: "[wmdb-lock] js-action waited %.0fms for sem (prev holder: %@)", waitMs, holderBeforeWait))
            }
        }
        setWriterHolder("js-action")
        defer {
            clearWriterHolder()
            writerTransactionSemaphore.signal()
        }

        guard writer.beginTransaction() else {
            let err = writer.lastError()
            if isBusyError(err.localizedDescription) {
                wmdbLockLog("[wmdb-lock] js-action BEGIN BUSY error=\(err.localizedDescription) holder=\(currentHolderInfo())")
            }
            throw err
        }

        // Atomically increment transaction depth
        transactionLock.lock()
        _transactionDepth += 1
        transactionLock.unlock()

        defer {
            // Atomically decrement transaction depth
            transactionLock.lock()
            _transactionDepth -= 1
            transactionLock.unlock()
        }

        do {
            try executeBlock()
            guard writer.commit() else { throw writer.lastError() }
        } catch {
            guard (error as NSError).code != SQLITE_FULL else {
                throw error
            }

            guard writer.rollback() else {
                throw writer.lastError()
            }

            throw error
        }
    }

    func execute(_ query: SQL, _ args: QueryArgs = []) throws {
        do {
            try writer.executeUpdate(query, values: args)
        } catch {
            let nsErr = error as NSError
            if WMDBLockLog.isEnabled && isBusyError(nsErr.localizedDescription) {
                let truncatedQuery = query.count > 80 ? String(query.prefix(80)) + "…" : query
                wmdbLockLog("[wmdb-lock] execute BUSY error=\(nsErr.localizedDescription) query=\"\(truncatedQuery)\" holder=\(currentHolderInfo())")
            }
            throw error
        }
    }

    /// Executes multiple queries separated by `;`
    func executeStatements(_ queries: SQL) throws {
        guard writer.executeStatements(queries) else {
            let err = writer.lastError()
            if WMDBLockLog.isEnabled && isBusyError(err.localizedDescription) {
                let truncated = queries.count > 80 ? String(queries.prefix(80)) + "…" : queries
                wmdbLockLog("[wmdb-lock] executeStatements BUSY error=\(err.localizedDescription) queries=\"\(truncated)\" holder=\(currentHolderInfo())")
            }
            throw err
        }
    }

    /// Serialized single-statement writer entry point. Acquires
    /// `writerTransactionSemaphore` WITHOUT opening a transaction, so callers
    /// that cannot run inside a txn (ATTACH/DETACH/VACUUM) are still serialized
    /// against in-flight `inTransaction` / slice-import writes on the shared
    /// writer connection (MOBILE-5606). MUST NOT be called from inside
    /// `inTransaction` — `writerTransactionSemaphore` is non-reentrant.
    func executeStandalone(_ query: SQL, _ args: QueryArgs = []) throws {
        writerTransactionSemaphore.wait()
        setWriterHolder("standalone")
        defer {
            clearWriterHolder()
            writerTransactionSemaphore.signal()
        }
        try execute(query, args)
    }

    /// Serialized multi-statement variant of `executeStandalone`. Same
    /// preconditions: acquires the semaphore, opens no transaction, and MUST
    /// NOT be called while the semaphore is already held.
    func executeStatementsStandalone(_ queries: SQL) throws {
        writerTransactionSemaphore.wait()
        setWriterHolder("standalone")
        defer {
            clearWriterHolder()
            writerTransactionSemaphore.signal()
        }
        try executeStatements(queries)
    }

    func getRawPointer() -> OpaquePointer {
        return OpaquePointer(writer.sqliteHandle)
    }
    
    func getRawReadPointer() -> OpaquePointer {
        return OpaquePointer(reader.sqliteHandle)
    }
    
    func setUpdateHook(withCallback callback: @escaping (UnsafeMutableRawPointer?, Int32, UnsafePointer<Int8>?, UnsafePointer<Int8>?, Int64) -> Void) {
        self.updateHookCallback = callback

        let sqliteHandle = OpaquePointer(writer.sqliteHandle)
        let userData: UnsafeMutableRawPointer? = UnsafeMutableRawPointer(Unmanaged.passUnretained(self).toOpaque())

        sqlite3_update_hook(sqliteHandle, { (userData, opcode, dbName, tableName, rowId) -> Void in
            guard let userData = userData else { return }
            let handler = Unmanaged<Database>.fromOpaque(userData).takeUnretainedValue()

            // Call the instance's callback method
            handler.updateHookCallback?(userData, opcode, dbName, tableName, rowId)
        }, userData)
    }

    func disableUpdateHook() {
        // Guard against invalid database state.
        // IMPORTANT: Do NOT call writer.open() here — FMDatabase.open() actually
        // opens the database if closed, causing a resource leak during deinit.
        // Instead, check the raw handle which is nil when the connection is closed.
        guard writer.sqliteHandle != nil else {
            self.updateHookCallback = nil
            return
        }

        let sqliteHandle = OpaquePointer(writer.sqliteHandle)
        // Per SQLite docs: pass NULL to disable the hook
        sqlite3_update_hook(sqliteHandle, nil, nil)
        self.updateHookCallback = nil
    }
    
    func queryRaw(_ query: SQL, _ args: QueryArgs = []) throws -> AnyIterator<FMResultSet> {
        let resultSet = try readDatabase(for: query).executeQuery(query, values: args)
        
        return AnyIterator {
            if resultSet.next() {
                return resultSet
            } else {
                resultSet.close()
                return nil
            }
        }
    }

    func queryRawOnWriter(_ query: SQL, _ args: QueryArgs = []) throws -> AnyIterator<FMResultSet> {
        let resultSet = try writer.executeQuery(query, values: args)

        return AnyIterator {
            if resultSet.next() {
                return resultSet
            } else {
                resultSet.close()
                return nil
            }
        }
    }
    
    /// Use `select count(*) as count`
    func count(_ query: SQL, _ args: QueryArgs = []) throws -> Int {
        let result = try readDatabase(for: query).executeQuery(query, values: args)
        defer { result.close() }
        
        guard result.next() else {
            throw "Invalid count query, can't find next() on the result".asError()
        }
        
        guard result.columnIndex(forName: "count") != -1 else {
            throw "Invalid count query, can't find `count` column".asError()
        }
        
        return Int(result.int(forColumn: "count"))
    }
    
    var userVersion: Int {
        get {
            // swiftlint:disable:next force_try
            let result = try! writer.executeQuery("pragma user_version", values: [])
            result.next()
            defer { result.close() }
            return result.long(forColumnIndex: 0)
        }
        set {
            // PRECONDITION: caller must hold `writerTransactionSemaphore` (i.e.
            // run inside `inTransaction`). Uses bare `execute`; do NOT make it
            // self-acquire — setUpSchema/migrate set this from inside
            // `inTransaction`, so acquiring here would deadlock (MOBILE-5606).
            // swiftlint:disable:next force_try
            try! execute("pragma user_version = \(newValue)")
        }
    }
    
    func unsafeDestroyEverything() throws {
        // MOBILE-5606: hold the writer semaphore across the entire teardown so a
        // reset/logout (unsafeResetDatabase) cannot vacuum, close the connection,
        // or delete DB files while an `inTransaction` / slice-import write is in
        // flight on the shared writer connection. wait() blocks until any
        // in-flight transaction releases. Inner writes stay BARE
        // (execute/executeStatements) — we already hold the semaphore; the
        // *Standalone variants would deadlock here.
        writerTransactionSemaphore.wait()
        setWriterHolder("reset")
        defer {
            clearWriterHolder()
            writerTransactionSemaphore.signal()
        }

        // NOTE: Deleting files by default because it seems simpler, more reliable
        // But sadly this won't work for in-memory (shared) databases
        if isInMemoryDatabase {
            // NOTE: As of iOS 14, selecting tables from sqlite_master and deleting them does not work
            // They seem to be enabling "defensive" config. So we use another obscure method to clear the database
            // https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigresetdatabase
            
            guard watermelondb_sqlite_dbconfig_reset_database(OpaquePointer(writer.sqliteHandle), true) else {
                throw "Failed to enable reset database mode".asError()
            }
            
            try executeStatements("vacuum")
            
            guard watermelondb_sqlite_dbconfig_reset_database(OpaquePointer(writer.sqliteHandle), false) else {
                throw "Failed to disable reset database mode".asError()
            }
        } else {
            guard writer.close() else {
                throw "Could not close database".asError()
            }
            
            if reader !== writer {
                guard reader.close() else {
                    throw "Could not close database".asError()
                }
            }

            // MOBILE-5606: reopen on EVERY exit, including a thrown removeItem,
            // so we never return — and never release writerTransactionSemaphore
            // (held by the caller) — with the FMDB handles closed. Otherwise the
            // next waiter could resume on a closed connection. open() recreates
            // an empty DB if the files are gone, and reopens whatever remains if
            // a removal failed mid-reset.
            defer { open() }

            let manager = FileManager.default

            try manager.removeItem(atPath: path)

            func removeIfExists(_ path: String) throws {
                if manager.fileExists(atPath: path) {
                    try manager.removeItem(atPath: path)
                }
            }

            try removeIfExists("\(path)-wal")
            try removeIfExists("\(path)-shm")
        }
    }
    
    private func readDatabase(for query: SQL) -> FMDatabase {
        if transactionDepth > 0 {
            return writer
        }
        if !isReadOnlyQuery(query) {
            return writer
        }
        // Temp tables are per-connection and only exist on the writer.
        // Route queries referencing temp tables to the writer connection.
        if referencesTemporaryTable(query) {
            return writer
        }
        return reader
    }

    private func isReadOnlyQuery(_ query: SQL) -> Bool {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmed.hasPrefix("select") || trimmed.hasPrefix("with") || trimmed.hasPrefix("explain")
    }

    private func referencesTemporaryTable(_ query: SQL) -> Bool {
        let lower = query.lowercased()
        return lower.contains("temp.") || lower.contains("sqlite_temp_master")
    }
    
    private func setWalMode(on db: FMDatabase) throws {
        // Must be first — if another connection holds a lock, all subsequent
        // PRAGMAs (including journal_mode) would throw immediately.
        try db.executeQuery("pragma busy_timeout=5000", values: []).close()

        let result = try db.executeQuery("pragma journal_mode=wal", values: [])
        result.close()

        // Only set performance pragmas on writer connection
        if db === writer {
            // Critical performance settings for WAL mode
            try db.executeQuery("pragma synchronous=NORMAL", values: []).close()  // FULL is too slow, NORMAL is safe with WAL
            try db.executeQuery("pragma temp_store=MEMORY", values: []).close()   // Faster temp operations
            try db.executeQuery("pragma mmap_size=268435456", values: []).close() // 256MB memory-mapped I/O
        }
    }
    
    private func setQueryOnly(on db: FMDatabase) throws {
        let result = try db.executeQuery("pragma query_only=1", values: [])
        result.close()
    }
    
    private var isInMemoryDatabase: Bool {
        return Database.isInMemory(path: path)
    }
    
    private static func isInMemory(path: String) -> Bool {
        return path == ":memory:" || path == "file::memory:" || path.contains("?mode=memory")
    }

#if DEBUG
    func _test_readDatabaseIdentity(_ query: SQL) -> String {
        return readDatabase(for: query) === reader ? "reader" : "writer"
    }

    func _test_isReadOnlyQuery(_ query: SQL) -> Bool {
        return isReadOnlyQuery(query)
    }

    func _test_referencesTemporaryTable(_ query: SQL) -> Bool {
        return referencesTemporaryTable(query)
    }

    func _test_readerQueryOnlyValue() -> Int {
        let result = try! reader.executeQuery("pragma query_only", values: [])
        result.next()
        defer { result.close() }
        return Int(result.long(forColumnIndex: 0))
    }
#endif
}
