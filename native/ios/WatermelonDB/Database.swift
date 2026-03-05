import Foundation
import SQLite3

public class Database {
    public typealias SQL = String
    public typealias TableName = String
    public typealias QueryArgs = [Any]
    
    private let writer: FMDatabase
    private let reader: FMDatabase
    private let syncWriter: FMDatabase
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
    
    init(path: String) {
        self.path = path
        writer = FMDatabase(path: path)
        if Database.isInMemory(path: path) {
            reader = writer
            syncWriter = writer
        } else {
            reader = FMDatabase(path: path)
            syncWriter = FMDatabase(path: path)
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
        if syncWriter !== writer {
            syncWriter.close()
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

        if syncWriter !== writer {
            guard syncWriter.open() else {
                fatalError("Failed to open the sync writer database. \(syncWriter.lastErrorMessage())")
            }
        }

        do {
            try setWalMode(on: writer)
            if reader !== writer {
                try setWalMode(on: reader)
                try setQueryOnly(on: reader)
            }
            if syncWriter !== writer {
                try setWalMode(on: syncWriter)
            }
        } catch {
            fatalError("Failed to configure database connections \(error)")
        }

        // Set busy timeout on writer and syncWriter so concurrent writers
        // wait (via sqlite3_busy_timeout) instead of failing with SQLITE_BUSY
        sqlite3_busy_timeout(writer.sqliteHandle, 30000)
        if syncWriter !== writer {
            sqlite3_busy_timeout(syncWriter.sqliteHandle, 30000)
        }

        consoleLog("Opened database at: \(path)")
    }
    
    func inTransaction(_ executeBlock: () throws -> Void) throws {
        guard writer.beginTransaction() else { throw writer.lastError() }
        
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
        try writer.executeUpdate(query, values: args)
    }
    
    /// Executes multiple queries separated by `;`
    func executeStatements(_ queries: SQL) throws {
        guard writer.executeStatements(queries) else {
            throw writer.lastError()
        }
    }
    
    func getRawPointer() -> OpaquePointer {
        return OpaquePointer(writer.sqliteHandle)
    }
    
    func getRawReadPointer() -> OpaquePointer {
        return OpaquePointer(reader.sqliteHandle)
    }

    func getRawSyncPointer() -> OpaquePointer {
        return OpaquePointer(syncWriter.sqliteHandle)
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
            // swiftlint:disable:next force_try
            try! execute("pragma user_version = \(newValue)")
        }
    }
    
    func unsafeDestroyEverything() throws {
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

            if syncWriter !== writer {
                guard syncWriter.close() else {
                    throw "Could not close sync writer database".asError()
                }
            }

            let manager = FileManager.default
            
            try manager.removeItem(atPath: path)
            
            func removeIfExists(_ path: String) throws {
                if manager.fileExists(atPath: path) {
                    try manager.removeItem(atPath: path)
                }
            }
            
            try removeIfExists("\(path)-wal")
            try removeIfExists("\(path)-shm")
            
            open()
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
