import Foundation
import SQLite3

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
        return isReadOnlyQuery(query) ? reader : writer
    }

    private func isReadOnlyQuery(_ query: SQL) -> Bool {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmed.hasPrefix("select") || trimmed.hasPrefix("with") || trimmed.hasPrefix("explain")
    }
    
    private func setWalMode(on db: FMDatabase) throws {
        let result = try db.executeQuery("pragma journal_mode=wal", values: [])
        result.close()
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

    func _test_readerQueryOnlyValue() -> Int {
        let result = try! reader.executeQuery("pragma query_only", values: [])
        result.next()
        defer { result.close() }
        return Int(result.long(forColumnIndex: 0))
    }
#endif
}
