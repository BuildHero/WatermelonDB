// MOBILE-5606 — deterministic writer-serialization tests for the iOS
// `Database` class.
//
// Exercises the REAL `Database` (Database.swift) off-simulator on the macOS
// host: it is pure Foundation + SQLite3 + FMDB, so no React/JSI/Nitro and no
// device are required. Built + run by `run-tests.sh` (swiftc + clang).
//
// What it pins (the MOBILE-5606 fix = standalone writes acquire
// `writerTransactionSemaphore` via `executeStandalone`):
//   Test 1  — `executeStandalone` BLOCKS while a transaction holds the writer
//             semaphore. Reverting it to bare `execute` fails this.
//   Test 2a — a standalone write SURVIVES a concurrent transaction ROLLBACK.
//             This is the fix: the write serializes after the rollback instead
//             of co-mingling with it.
//   Test 2b — a BARE `execute` issued while a transaction is open lands INSIDE
//             that transaction and is LOST on rollback. This characterizes the
//             bug the fix prevents (shared single writer connection).
//   Test 3a-c — `copyTablesOnRawWriter` (MOBILE-7780, off-JS-thread snapshot
//             copy): same result as `DatabaseDriver.copyTables`, waits for the
//             writer semaphore, and rolls back + detaches + releases on failure.
//   Test 4a-c — (characterization) `setWalMode` never steps
//             `pragma journal_mode=wal`, so the database stays in rollback mode
//             and a reader gets SQLITE_BUSY behind any write that spills the page
//             cache, including `copyTablesOnRawWriter`. WAL stays off on purpose;
//             the JS dispatcher holds other calls until the off-thread copy ends.
//             4c: an FMDB read (`queryRaw`, behind find/getLocal) that hits that
//             SQLITE_BUSY returns NO rows instead of throwing.
//   Test 5  — a JS reload opens the file again while the old runtime's off-thread
//             copy runs: the new open cancels the copy (rolled back) instead of
//             dying on "database is locked" in `open()`.
//   Test 6-12 — review findings: a cancelled copy keeps the schema-version key;
//             a reopen skips copies still queued on the semaphore and waits at
//             most `reopenCopyWaitTimeout` for one holding the writer; every copy
//             on the path is cancelled; a superseded connection can't start a
//             copy; cancels between tables and before COMMIT roll back; reopens
//             with no running copy don't wait.
//   Test 13 — `open()` retries "database is locked" from another writer
//             instead of `fatalError`.
//   Test 14-15 — that retry starts no attempt after its deadline, so it gives up
//             within the deadline plus one busy timeout (simulated clock, then a
//             real lock).
//   Test 16 — a copy registered before the bridge's async hop is cancelled by a
//             reopen through its ticket, and a finished one leaves no entry.
//
// Pass test ids (`6 7`) to run a subset.
//
// Exit code 0 = all pass; non-zero = failure (CI-usable).

import Foundation
import SQLite3

// std_ext's `consoleLog` routes through this hook; silence it for the test.
_watermelonDBLoggingHook = { _ in }

private var failures = 0

private func check(_ condition: Bool, _ message: String) {
    if condition {
        print("  ✓ \(message)")
    } else {
        print("  ✗ FAIL: \(message)")
        failures += 1
    }
}

private var tempPaths: [String] = []

private func tempDBPath() -> String {
    let name = "wmdb-5606-\(UUID().uuidString).db"
    let path = (NSTemporaryDirectory() as NSString).appendingPathComponent(name)
    tempPaths.append(path)
    return path
}

private func removeTempDBs() {
    for path in tempPaths {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            try? FileManager.default.removeItem(atPath: path + suffix)
        }
    }
}

private func makeDB() -> Database {
    let db = Database(path: tempDBPath())
    // Single-threaded setup — safe to call the bare statement runner directly.
    try! db.executeStatements("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
    return db
}

private func count(_ db: Database, whereV v: String) -> Int {
    return (try? db.count("SELECT count(*) as count FROM t WHERE v = '\(v)'")) ?? -1
}

// ---------------------------------------------------------------------------
// Test 1 — executeStandalone serializes behind an in-flight transaction.
// ---------------------------------------------------------------------------
private func test1_standaloneBlocksWhileTransactionHeld() {
    print("Test 1: executeStandalone blocks while a transaction holds the writer semaphore")
    let db = makeDB()

    let txnHeld = DispatchSemaphore(value: 0)
    let releaseTxn = DispatchSemaphore(value: 0)
    let bFinished = DispatchSemaphore(value: 0)
    var bCompleted = false

    // Thread A: hold an open transaction → holds writerTransactionSemaphore.
    DispatchQueue.global().async {
        try? db.inTransaction {
            txnHeld.signal()
            releaseTxn.wait() // keep the transaction (and the semaphore) open
        }
    }
    txnHeld.wait()

    // Thread B: a standalone write. With the fix it must wait on the semaphore.
    DispatchQueue.global().async {
        try? db.executeStandalone("INSERT INTO t (v) VALUES ('B')")
        bCompleted = true
        bFinished.signal()
    }

    // If standalone writes weren't serialized, B would complete in this window.
    Thread.sleep(forTimeInterval: 0.3)
    check(!bCompleted, "standalone write did NOT complete while the transaction held the semaphore")

    // Release the transaction; B should now proceed and persist.
    releaseTxn.signal()
    check(bFinished.wait(timeout: .now() + 5) == .success, "standalone write completed after the transaction released")
    check(count(db, whereV: "B") == 1, "standalone write persisted (count == 1)")
}

// ---------------------------------------------------------------------------
// Test 2a — standalone write survives a concurrent transaction ROLLBACK (FIX).
// ---------------------------------------------------------------------------
private func test2a_standaloneSurvivesConcurrentRollback() {
    print("Test 2a (FIX): standalone write survives a concurrent transaction ROLLBACK")
    let db = makeDB()

    let txnHeld = DispatchSemaphore(value: 0)
    let releaseTxn = DispatchSemaphore(value: 0)
    let bWritten = DispatchSemaphore(value: 0)

    // Thread A: open a transaction, write an uncommitted row, then ROLL BACK
    // (inTransaction rolls back when its body throws).
    DispatchQueue.global().async {
        try? db.inTransaction {
            try db.execute("INSERT INTO t (v) VALUES ('A-uncommitted')")
            txnHeld.signal()
            releaseTxn.wait()
            throw "force rollback".asError()
        }
    }
    txnHeld.wait()

    // Thread B: standalone write. With the fix it blocks on the semaphore until
    // A rolls back and releases, then writes in its own autocommit.
    DispatchQueue.global().async {
        try? db.executeStandalone("INSERT INTO t (v) VALUES ('B-standalone')")
        bWritten.signal()
    }

    releaseTxn.signal() // A throws → rolls back → releases the semaphore
    _ = bWritten.wait(timeout: .now() + 5)
    Thread.sleep(forTimeInterval: 0.1) // let the autocommit settle

    check(count(db, whereV: "A-uncommitted") == 0, "A's row was rolled back (count == 0)")
    check(count(db, whereV: "B-standalone") == 1, "B's standalone write SURVIVED the rollback (count == 1)")
}

// ---------------------------------------------------------------------------
// Test 2b — a BARE execute is LOST on rollback (characterizes the bug).
// ---------------------------------------------------------------------------
private func test2b_bareExecuteLostOnConcurrentRollback() {
    print("Test 2b (BUG): a bare execute lands inside the open transaction and is LOST on rollback")
    let db = makeDB()

    let txnHeld = DispatchSemaphore(value: 0)
    let releaseTxn = DispatchSemaphore(value: 0)
    let bWritten = DispatchSemaphore(value: 0)

    DispatchQueue.global().async {
        try? db.inTransaction {
            txnHeld.signal()
            releaseTxn.wait()
            throw "force rollback".asError()
        }
    }
    txnHeld.wait()

    // BARE execute (the pre-fix path): no semaphore, so it runs on the shared
    // writer connection WHILE the transaction is open → becomes part of it. We
    // sequence B to complete BEFORE releasing A, so the write deterministically
    // lands inside the transaction.
    DispatchQueue.global().async {
        try? db.execute("INSERT INTO t (v) VALUES ('B-bare')")
        bWritten.signal()
    }
    _ = bWritten.wait(timeout: .now() + 5)

    releaseTxn.signal() // A rolls back, taking B's co-mingled write with it
    Thread.sleep(forTimeInterval: 0.1)

    check(count(db, whereV: "B-bare") == 0,
          "bare execute was LOST on rollback (count == 0) — the MOBILE-5606 race the fix prevents")
}

// ---------------------------------------------------------------------------
// Test 3 — copyTablesOnRawWriter (MOBILE-7780).
// ---------------------------------------------------------------------------
private func makeSnapshotSource() -> String {
    let path = tempDBPath()
    let src = Database(path: path)
    try! src.executeStatements("""
        CREATE TABLE local_storage (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO local_storage VALUES ('__watermelon_last_pulled_schema_version', '99'), ('cursor', 'abc');
        CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, onlyInSource TEXT);
        INSERT INTO tasks VALUES ('t1', 'from-snapshot', 'x'), ('t2', 'two', 'y');
        CREATE TABLE projects (id TEXT PRIMARY KEY, title TEXT);
        INSERT INTO projects VALUES ('p1', 'project');
        """)
    src.close()
    return path
}

private func makeCopyTarget() -> Database {
    let db = Database(path: tempDBPath())
    try! db.executeStatements("""
        CREATE TABLE local_storage (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO local_storage VALUES ('__watermelon_last_pulled_schema_version', '7');
        CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT, onlyInMain TEXT);
        INSERT INTO tasks VALUES ('t1', 'already-local', 'kept');
        CREATE TABLE projects (id TEXT PRIMARY KEY, title TEXT);
        """)
    return db
}

private func scalar(_ db: Database, _ sql: String) -> String? {
    guard let iter = try? db.queryRaw(sql), let row = iter.next() else { return nil }
    let value = row.string(forColumnIndex: 0)
    while iter.next() != nil {}
    return value
}

private func isOtherAttached(_ db: Database) -> Bool {
    guard let iter = try? db.queryRawOnWriter("PRAGMA database_list") else { return false }
    var attached = false
    while let row = iter.next() {
        if row.string(forColumn: "name") == "other" { attached = true }
    }
    return attached
}

private func test3a_copyMatchesDriverCopyTables() {
    print("Test 3a: copyTablesOnRawWriter copies common columns, keeps local rows, drops the schema-version key")
    let db = makeCopyTarget()
    let src = makeSnapshotSource()

    do {
        try db.copyTablesOnRawWriter(["local_storage", "tasks", "projects", "missing_table"], srcDB: src)
        check(true, "copy succeeded")
    } catch {
        check(false, "copy threw: \(error)")
    }

    check(scalar(db, "SELECT name FROM tasks WHERE id = 't1'") == "already-local", "INSERT OR IGNORE kept the existing local row")
    check(scalar(db, "SELECT name FROM tasks WHERE id = 't2'") == "two", "new row copied")
    check(scalar(db, "SELECT count(*) FROM projects") == "1", "second table copied")
    check(scalar(db, "SELECT value FROM local_storage WHERE key = 'cursor'") == "abc", "local_storage rows copied")
    check(scalar(db, "SELECT count(*) FROM local_storage WHERE key = '__watermelon_last_pulled_schema_version'") == "1"
          && scalar(db, "SELECT value FROM local_storage WHERE key = '__watermelon_last_pulled_schema_version'") == "99",
          "schema-version key was deleted locally before the copy (source value wins, local '7' is gone)")
    check(!isOtherAttached(db), "source database detached afterwards")
}

private func test3b_copyWaitsForWriterSemaphore() {
    print("Test 3b: copyTablesOnRawWriter waits while a transaction holds the writer semaphore")
    let db = makeCopyTarget()
    let src = makeSnapshotSource()

    let txnHeld = DispatchSemaphore(value: 0)
    let releaseTxn = DispatchSemaphore(value: 0)
    let copyFinished = DispatchSemaphore(value: 0)
    var copyCompleted = false

    DispatchQueue.global().async {
        try? db.inTransaction {
            txnHeld.signal()
            releaseTxn.wait()
        }
    }
    txnHeld.wait()

    DispatchQueue.global().async {
        try? db.copyTablesOnRawWriter(["tasks"], srcDB: src)
        copyCompleted = true
        copyFinished.signal()
    }

    Thread.sleep(forTimeInterval: 0.3)
    check(!copyCompleted, "copy did NOT run while the transaction held the semaphore")

    releaseTxn.signal()
    check(copyFinished.wait(timeout: .now() + 5) == .success, "copy completed after the transaction released")
    check(scalar(db, "SELECT count(*) FROM tasks") == "2", "copied rows visible (count == 2)")
}

private func test3c_failedCopyRollsBackAndReleases() {
    print("Test 3c: a failing copy rolls back every table, detaches, and releases the semaphore")
    let db = makeCopyTarget()
    let src = makeSnapshotSource()
    try! db.executeStatements("CREATE TRIGGER fail_projects BEFORE INSERT ON projects BEGIN SELECT RAISE(ABORT, 'boom'); END")

    var thrown: Error?
    do {
        try db.copyTablesOnRawWriter(["tasks", "projects"], srcDB: src)
    } catch {
        thrown = error
    }

    check(thrown != nil, "copy threw")
    check(scalar(db, "SELECT count(*) FROM tasks") == "1", "rows copied before the failure were rolled back (count == 1)")
    check(!isOtherAttached(db), "source database detached after the failure")

    let standaloneFinished = DispatchSemaphore(value: 0)
    DispatchQueue.global().async {
        try? db.executeStandalone("INSERT INTO tasks (id, name) VALUES ('after', 'after')")
        standaloneFinished.signal()
    }
    check(standaloneFinished.wait(timeout: .now() + 5) == .success, "writer semaphore released (a later standalone write ran)")
}

print("MOBILE-5606 writer-serialization tests")
// ---------------------------------------------------------------------------
// Test 4 — the database is in rollback mode, so readers block on writers.
// ---------------------------------------------------------------------------
private func rawScalar(_ handle: OpaquePointer, _ sql: String) -> (rc: Int32, value: String?) {
    var stmt: OpaquePointer?
    var rc = sqlite3_prepare_v2(handle, sql, -1, &stmt, nil)
    defer { sqlite3_finalize(stmt) }
    guard rc == SQLITE_OK else { return (rc, nil) }
    rc = sqlite3_step(stmt)
    let value = rc == SQLITE_ROW ? sqlite3_column_text(stmt, 0).map { String(cString: $0) } : nil
    return (rc, value)
}

/// Opens a write transaction on the raw writer that spills the page cache (as the snapshot copy does),
/// then reads on the reader connection with a short busy timeout. Returns the reader's step result code.
private func readWhileSpilledWriteIsOpen(_ db: Database) -> Int32 {
    let writer = db.getRawPointer()
    let reader = db.getRawReadPointer()
    sqlite3_busy_timeout(reader, 200)
    _ = rawScalar(reader, "SELECT count(*) FROM t") // load the schema on the reader, as a running app has
    _ = rawScalar(writer, "pragma cache_size=5")
    _ = rawScalar(writer, "pragma cache_spill=5")
    sqlite3_exec(writer, "BEGIN IMMEDIATE", nil, nil, nil)
    sqlite3_exec(writer, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5000) INSERT INTO t (v) SELECT hex(randomblob(100)) FROM c", nil, nil, nil)
    let read = rawScalar(reader, "SELECT count(*) FROM t")
    sqlite3_exec(writer, "ROLLBACK", nil, nil, nil)
    return read.rc
}

private func test4a_databaseOpensInRollbackMode() {
    print("Test 4a: Database(path:) leaves the file in rollback mode, not WAL")
    let db = makeDB()
    check(rawScalar(db.getRawPointer(), "pragma journal_mode").value == "delete",
          "journal_mode is 'delete' after open: executeQuery prepares 'pragma journal_mode=wal' but never steps it")
}

private func test4b_readerBlocksBehindSpilledWrite() {
    print("Test 4b: in rollback mode a reader gets SQLITE_BUSY behind a write that spilled the cache")
    let db = makeDB()
    check(readWhileSpilledWriteIsOpen(db) == SQLITE_BUSY, "reader read failed with SQLITE_BUSY (database is locked)")
}

private func test4c_fmdbReadReturnsNoRowsWhenBusy() {
    print("Test 4c: an FMDB reader read that hits SQLITE_BUSY returns no rows instead of throwing")
    let db = makeDB()
    try! db.executeStatements("INSERT INTO t (v) VALUES ('committed')")
    let writer = db.getRawPointer()
    sqlite3_busy_timeout(db.getRawReadPointer(), 200)
    _ = rawScalar(db.getRawReadPointer(), "SELECT count(*) FROM t")
    _ = rawScalar(writer, "pragma cache_size=5")
    _ = rawScalar(writer, "pragma cache_spill=5")
    sqlite3_exec(writer, "BEGIN IMMEDIATE", nil, nil, nil)
    sqlite3_exec(writer, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5000) INSERT INTO t (v) SELECT hex(randomblob(100)) FROM c", nil, nil, nil)
    var rows = 0
    var threw = false
    do {
        let iter = try db.queryRaw("SELECT v FROM t WHERE v = 'committed'")
        while iter.next() != nil { rows += 1 }
    } catch {
        threw = true
    }
    sqlite3_exec(writer, "ROLLBACK", nil, nil, nil)
    check(!threw && rows == 0, "queryRaw returned 0 rows and no error while the committed row exists (rows=\(rows), threw=\(threw))")
    check(count(db, whereV: "committed") == 1, "the committed row is there once the write ends")
}

// ---------------------------------------------------------------------------
// Test 5 — a JS reload opens the file again while the old runtime's copy runs.
// ---------------------------------------------------------------------------
private func test5_reopenDuringCopyCancelsIt() {
    print("Test 5: opening the same file while an off-thread copy runs cancels the copy instead of failing to open")
    let srcPath = tempDBPath()
    let src = Database(path: srcPath)
    try! src.executeStatements("""
        CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT);
        WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3000000)
        INSERT INTO tasks SELECT hex(randomblob(16)), hex(randomblob(40)) FROM c;
        """)
    src.close()

    let targetPath = tempDBPath()
    let target = Database(path: targetPath)
    try! target.executeStatements("""
        CREATE TABLE local_storage (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT);
        INSERT INTO tasks VALUES ('local', 'kept');
        """)
    // A small cache makes the copy spill early, as the device copy does on a real snapshot.
    _ = rawScalar(target.getRawPointer(), "pragma cache_size=50")

    var copyError: Error?
    let copyDone = DispatchSemaphore(value: 0)
    let insertRunning = DispatchSemaphore(value: 0)
    var ticks = 0
    Database._test_onProgressTick = {
        ticks += 1
        if ticks == 200 { insertRunning.signal() }
    }
    defer { Database._test_onProgressTick = nil }
    DispatchQueue.global().async {
        do {
            try target.copyTablesOnRawWriter(["tasks"], srcDB: srcPath)
        } catch {
            copyError = error
        }
        copyDone.signal()
    }
    check(insertRunning.wait(timeout: .now() + 30) == .success, "the copy's INSERT is running (200 progress ticks)")

    let openStart = Date()
    let reopened = Database(path: targetPath)
    let openSeconds = Date().timeIntervalSince(openStart)
    copyDone.wait()

    check((copyError as NSError?)?.code == Int(SQLITE_INTERRUPT),
          "the in-flight copy was cancelled with SQLITE_INTERRUPT (error: \(String(describing: copyError)))")
    check(openSeconds < 3, "the new connection opened in \(String(format: "%.2f", openSeconds))s, without waiting out the copy")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "1", "the cancelled copy rolled back: only the local row remains")
    check((try? reopened.executeStatements("INSERT INTO tasks VALUES ('after', 'reopen')")) != nil,
          "the new connection can write once the copy is gone")
}


// ---------------------------------------------------------------------------
// Test 6-12 — review findings on the off-thread copy (MOBILE-7780).
// ---------------------------------------------------------------------------
private func makeBigSource(rows: Int) -> String {
    let path = tempDBPath()
    let src = Database(path: path)
    try! src.executeStatements("""
        CREATE TABLE local_storage (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO local_storage VALUES ('__watermelon_last_pulled_schema_version', '99');
        CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT);
        WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < \(rows))
        INSERT INTO tasks SELECT hex(randomblob(16)), hex(randomblob(40)) FROM c;
        """)
    src.close()
    return path
}

private func makeReopenTarget() -> (Database, String) {
    let path = tempDBPath()
    let db = Database(path: path)
    try! db.executeStatements("""
        CREATE TABLE local_storage (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO local_storage VALUES ('__watermelon_last_pulled_schema_version', '7'), ('keep', 'me');
        CREATE TABLE tasks (id TEXT PRIMARY KEY, name TEXT);
        INSERT INTO tasks VALUES ('local', 'kept');
        """)
    _ = rawScalar(db.getRawPointer(), "pragma cache_size=50")
    return (db, path)
}

private final class CopyRun {
    var error: Error?
    let done = DispatchSemaphore(value: 0)
}

private func runCopy(_ db: Database, _ tables: [String], _ src: String) -> CopyRun {
    let run = CopyRun()
    DispatchQueue.global().async {
        do {
            try db.copyTablesOnRawWriter(tables, srcDB: src)
        } catch {
            run.error = error
        }
        run.done.signal()
    }
    return run
}

private func test6_cancelledCopyKeepsSchemaVersionKey() {
    print("Test 6: a cancelled copy leaves the schema-version key and every row as they were")
    let (target, path) = makeReopenTarget()
    let src = makeBigSource(rows: 3_000_000)
    let insertRunning = DispatchSemaphore(value: 0)
    var ticks = 0
    Database._test_onProgressTick = {
        ticks += 1
        if ticks == 200 { insertRunning.signal() }
    }
    defer { Database._test_onProgressTick = nil }

    let run = runCopy(target, ["local_storage", "tasks"], src)
    _ = insertRunning.wait(timeout: .now() + 1) // falls back to ~1s into the copy on a build without the tick hook
    let reopened = Database(path: path)
    run.done.wait()

    check(run.error != nil, "the copy was cancelled")
    check(scalar(reopened, "SELECT value FROM local_storage WHERE key = '__watermelon_last_pulled_schema_version'") == "7",
          "schema-version key is still '7' (the DELETE rolled back with the copy)")
    check(scalar(reopened, "SELECT value FROM local_storage WHERE key = 'keep'") == "me", "other local_storage rows untouched")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "1", "no copied rows remain")
}

private func test7_reopenDoesNotWaitForAQueuedCopy() {
    print("Test 7: a reopen does not wait for a copy still queued on the writer semaphore, and that copy never runs")
    let (target, path) = makeReopenTarget()
    let src = makeSnapshotSource()

    let queued = DispatchSemaphore(value: 0)
    Database._test_onCopyStep = { step in
        if step == "queuedForWriter" { queued.signal() }
    }
    defer { Database._test_onCopyStep = nil }

    target.writerTransactionSemaphore.wait() // another writer (e.g. a slice import) holds the semaphore, no SQL
    let run = runCopy(target, ["tasks"], src)
    check(queued.wait(timeout: .now() + 5) == .success, "the copy registered and is parked on the semaphore")

    let openStart = Date()
    let reopened = Database(path: path)
    let openSeconds = Date().timeIntervalSince(openStart)
    check(openSeconds < 0.5, "reopen returned in \(String(format: "%.2f", openSeconds))s without waiting for the queued copy")

    target.writerTransactionSemaphore.signal()
    check(run.done.wait(timeout: .now() + 5) == .success, "the queued copy exited")
    let message = (run.error as NSError?)?.localizedDescription ?? ""
    check((run.error as NSError?)?.code == Int(SQLITE_INTERRUPT) && message.contains("before the copy started"),
          "the queued copy was cancelled while waiting, not refused at registration (error: \(message))")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "1", "the queued copy never wrote a row")
}

private func test8_reopenWaitIsBounded() {
    print("Test 8: a reopen waits at most reopenCopyWaitTimeout for a copy that does not respond to the cancel")
    let (target, path) = makeReopenTarget()
    let src = makeSnapshotSource()
    let previousTimeout = Database.reopenCopyWaitTimeout
    Database.reopenCopyWaitTimeout = 0.5
    let inStep = DispatchSemaphore(value: 0)
    Database._test_onCopyStep = { step in
        if step == "beforeTable:tasks" {
            inStep.signal()
            Thread.sleep(forTimeInterval: 2.0) // stuck holding the writer, not checking the cancel flag
        }
    }
    defer {
        Database._test_onCopyStep = nil
        Database.reopenCopyWaitTimeout = previousTimeout
    }

    let run = runCopy(target, ["tasks"], src)
    inStep.wait()
    let waitStart = Date()
    Database._test_cancelAndWaitForCopies(path: path)
    let waited = Date().timeIntervalSince(waitStart)
    run.done.wait()

    check(waited >= 0.4 && waited < 1.5, "reopen gave up waiting after \(String(format: "%.2f", waited))s (timeout 0.5s, copy stuck 2s)")
    check(run.error != nil, "the stuck copy still saw the cancel once it resumed")
}

private func test9_reopenCancelsEveryCopyOnThePath() {
    print("Test 9: with two copies on one connection, a reopen cancels the one still running after the other finished")
    let (target, path) = makeReopenTarget()
    let small = makeBigSource(rows: 300_000) // long enough that the second copy registers while it runs
    let big = makeBigSource(rows: 3_000_000)

    let bigRunning = DispatchSemaphore(value: 0)
    var ticks = 0
    Database._test_onProgressTick = {
        ticks += 1
        if ticks == 2000 { bigRunning.signal() }
    }
    defer { Database._test_onProgressTick = nil }

    let smallRun = runCopy(target, ["tasks"], small)
    Thread.sleep(forTimeInterval: 0.01)
    let bigRun = runCopy(target, ["tasks"], big)
    smallRun.done.wait()
    _ = bigRunning.wait(timeout: .now() + 1) // falls back to ~1s on a build without the tick hook

    let openStart = Date()
    let reopened = Database(path: path)
    let openSeconds = Date().timeIntervalSince(openStart)
    bigRun.done.wait()

    check(smallRun.error == nil, "the first copy finished normally")
    check((bigRun.error as NSError?)?.code == Int(SQLITE_INTERRUPT), "the still-running copy was cancelled (error: \(String(describing: bigRun.error)))")
    check(openSeconds < 3, "reopen took \(String(format: "%.2f", openSeconds))s")
    _ = reopened
}

private func test10_supersededConnectionCannotStartACopy() {
    print("Test 10: once the file is reopened, a copy started on the old connection is refused; the new one can copy")
    let (old, path) = makeReopenTarget()
    let src = makeSnapshotSource()
    let reopened = Database(path: path)

    var oldError: Error?
    do {
        try old.copyTablesOnRawWriter(["tasks"], srcDB: src)
    } catch {
        oldError = error
    }
    check(oldError != nil, "the old connection's copy was refused (error: \(String(describing: oldError)))")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "1", "the refused copy wrote nothing")

    var newError: Error?
    do {
        try reopened.copyTablesOnRawWriter(["tasks"], srcDB: src)
    } catch {
        newError = error
    }
    check(newError == nil, "the new connection's copy ran (error: \(String(describing: newError)))")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "3", "the new connection's copy committed")
}

private func test11_cancelBetweenTablesAndBeforeCommit() {
    for step in ["beforeTable:projects", "beforeCommit"] {
        print("Test 11 (\(step)): a cancel at this point rolls the whole copy back with SQLITE_INTERRUPT")
        let (target, path) = makeReopenTarget()
        try! target.executeStatements("CREATE TABLE projects (id TEXT PRIMARY KEY, title TEXT)")
        let src = makeSnapshotSource()
        Database._test_onCopyStep = { current in
            if current == step { Database._test_requestCancel(path: path) }
        }
        defer { Database._test_onCopyStep = nil }

        var copyError: Error?
        do {
            try target.copyTablesOnRawWriter(["tasks", "projects"], srcDB: src)
        } catch {
            copyError = error
        }
        check((copyError as NSError?)?.code == Int(SQLITE_INTERRUPT), "copy cancelled (error: \(String(describing: copyError)))")
        check(scalar(target, "SELECT count(*) FROM tasks") == "1", "tasks rolled back")
        check(scalar(target, "SELECT value FROM local_storage WHERE key = '__watermelon_last_pulled_schema_version'") == "7",
              "schema-version key kept")
        check(!isOtherAttached(target), "source detached")
    }
}

private func test12_reopenWithoutARunningCopy() {
    print("Test 12: reopening after a finished copy, with no copy, or on another path does not wait or cancel")
    let (target, path) = makeReopenTarget()
    let src = makeSnapshotSource()
    try! target.copyTablesOnRawWriter(["tasks"], srcDB: src)
    var start = Date()
    let afterFinished = Database(path: path)
    check(Date().timeIntervalSince(start) < 0.5, "reopen after a finished copy is immediate")
    check(scalar(afterFinished, "SELECT count(*) FROM tasks") == "3", "the finished copy's rows persisted")

    let (_, otherPath) = makeReopenTarget()
    start = Date()
    _ = Database(path: otherPath)
    check(Date().timeIntervalSince(start) < 0.5, "reopen with no copy is immediate")

    let (running, _) = makeReopenTarget()
    let big = makeBigSource(rows: 1_000_000)
    let run = runCopy(running, ["tasks"], big)
    Thread.sleep(forTimeInterval: 0.2)
    _ = Database(path: tempDBPath()) // a different file
    run.done.wait()
    check(run.error == nil, "a copy on another path was not cancelled (error: \(String(describing: run.error)))")
}

private func test13_openWaitsOutABusyWriter() {
    print("Test 13: opening while another connection holds the file locked past the busy timeout retries instead of crashing")
    let (target, path) = makeReopenTarget()
    target.close()
    var holder: OpaquePointer?
    sqlite3_open(path, &holder)
    sqlite3_exec(holder, "BEGIN EXCLUSIVE", nil, nil, nil)
    sqlite3_exec(holder, "INSERT INTO tasks VALUES ('held', 'x')", nil, nil, nil)
    DispatchQueue.global().asyncAfter(deadline: .now() + 6.5) {
        sqlite3_exec(holder, "COMMIT", nil, nil, nil)
        sqlite3_close(holder)
    }

    let start = Date()
    let reopened = Database(path: path)
    let seconds = Date().timeIntervalSince(start)
    check(seconds >= 6, "open waited for the lock holder (\(String(format: "%.2f", seconds))s, busy timeout 5s)")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "2", "the opened connection reads the holder's committed row")
}

private let busyError = NSError(domain: "FMDatabase", code: 5, userInfo: [NSLocalizedDescriptionKey: "database is locked"])

private func test14_openRetryEndsWithinDeadlinePlusOneBusyTimeout() {
    print("Test 14: open()'s BUSY retry starts no attempt after its deadline (simulated clock, deadlines 0-40s)")
    var worstOvershoot = -Double.infinity
    var worstDeadline = 0.0
    var bad = 0
    for step in 0...800 {
        let deadline = Double(step) * 0.05
        var clock: TimeInterval = 0
        var threw = false
        do {
            try Database.retryWhileBusy(for: deadline,
                                        now: { Date(timeIntervalSinceReferenceDate: clock) },
                                        sleep: { clock += $0 }) {
                clock += Database.busyTimeout // every attempt blocks for the whole busy timeout, then fails
                throw busyError
            }
        } catch {
            threw = true
        }
        if !threw || clock < deadline || clock > deadline + Database.busyTimeout + 1e-9 { bad += 1 }
        if clock - deadline > worstOvershoot {
            worstOvershoot = clock - deadline
            worstDeadline = deadline
        }
    }
    check(bad == 0, "gave up between the deadline and deadline + busy timeout for every deadline " +
          "(worst: \(String(format: "%.2f", worstOvershoot))s past a \(String(format: "%.2f", worstDeadline))s deadline)")
}

private func test15_openRetryGivesUpOnARealLock() {
    print("Test 15: against a file held EXCLUSIVE, the retry gives up within deadline + one busy timeout")
    let (target, path) = makeReopenTarget()
    target.close()
    var holder: OpaquePointer?
    sqlite3_open(path, &holder)
    sqlite3_exec(holder, "BEGIN EXCLUSIVE", nil, nil, nil)
    sqlite3_exec(holder, "INSERT INTO tasks VALUES ('held', 'x')", nil, nil, nil)
    defer {
        sqlite3_exec(holder, "ROLLBACK", nil, nil, nil)
        sqlite3_close(holder)
    }

    let probe = FMDatabase(path: path)
    probe.open()
    defer { probe.close() }
    let deadline: TimeInterval = 1
    let start = Date()
    var finalError: Error?
    do {
        try Database.retryWhileBusy(for: deadline) {
            try probe.executeQuery("pragma busy_timeout=\(Int(Database.busyTimeout * 1000))", values: []).close()
            try probe.executeQuery("pragma journal_mode=wal", values: []).close()
        }
    } catch {
        finalError = error
    }
    let seconds = Date().timeIntervalSince(start)
    check(finalError != nil, "gave up with the BUSY error (\(String(describing: finalError)))")
    check(seconds >= deadline && seconds <= deadline + Database.busyTimeout + 0.5,
          "gave up after \(String(format: "%.2f", seconds))s (deadline \(deadline)s + busy timeout \(Database.busyTimeout)s)")
}

private func test16_registeredCopyIsCancelledByAReopen() {
    print("Test 16: a copy registered before the async hop (the bridge path) is cancelled by a reopen before it starts")
    let (target, path) = makeReopenTarget()
    let src = makeSnapshotSource()
    let registered = try! target.registerOffThreadCopy()

    let openStart = Date()
    let reopened = Database(path: path)
    let openSeconds = Date().timeIntervalSince(openStart)
    check(openSeconds < 0.5, "reopen did not wait for a registered copy that has not started (\(String(format: "%.2f", openSeconds))s)")

    var copyError: Error?
    do {
        try target.copyTablesOnRawWriter(["tasks"], srcDB: src, registered: registered)
    } catch {
        copyError = error
    }
    let message = (copyError as NSError?)?.localizedDescription ?? ""
    check((copyError as NSError?)?.code == Int(SQLITE_INTERRUPT) && message.contains("before the copy started"),
          "the registered copy was cancelled through its ticket (error: \(message))")
    check(scalar(reopened, "SELECT count(*) FROM tasks") == "1", "the cancelled copy wrote nothing")

    let (finishing, finishingPath) = makeReopenTarget()
    let ticket = try! finishing.registerOffThreadCopy()
    try! finishing.copyTablesOnRawWriter(["tasks"], srcDB: src, registered: ticket)
    let afterStart = Date()
    let afterFinished = Database(path: finishingPath)
    check(Date().timeIntervalSince(afterStart) < 0.5, "a finished registered copy leaves no registry entry behind")
    check(scalar(afterFinished, "SELECT count(*) FROM tasks") == "3", "the registered copy committed")
}

private let allTests: [(String, () -> Void)] = [
    ("1", test1_standaloneBlocksWhileTransactionHeld),
    ("2a", test2a_standaloneSurvivesConcurrentRollback),
    ("2b", test2b_bareExecuteLostOnConcurrentRollback),
    ("3a", test3a_copyMatchesDriverCopyTables),
    ("3b", test3b_copyWaitsForWriterSemaphore),
    ("3c", test3c_failedCopyRollsBackAndReleases),
    ("4a", test4a_databaseOpensInRollbackMode),
    ("4b", test4b_readerBlocksBehindSpilledWrite),
    ("4c", test4c_fmdbReadReturnsNoRowsWhenBusy),
    ("5", test5_reopenDuringCopyCancelsIt),
    ("6", test6_cancelledCopyKeepsSchemaVersionKey),
    ("7", test7_reopenDoesNotWaitForAQueuedCopy),
    ("8", test8_reopenWaitIsBounded),
    ("9", test9_reopenCancelsEveryCopyOnThePath),
    ("10", test10_supersededConnectionCannotStartACopy),
    ("11", test11_cancelBetweenTablesAndBeforeCommit),
    ("12", test12_reopenWithoutARunningCopy),
    ("13", test13_openWaitsOutABusyWriter),
    ("14", test14_openRetryEndsWithinDeadlinePlusOneBusyTimeout),
    ("15", test15_openRetryGivesUpOnARealLock),
    ("16", test16_registeredCopyIsCancelledByAReopen),
]

// Pass test ids (e.g. `6 7`) to run a subset; no arguments runs everything.
let selected = Set(CommandLine.arguments.dropFirst())
for (id, test) in allTests where selected.isEmpty || selected.contains(id) {
    test()
}
removeTempDBs()

if failures == 0 {
    print("\nALL PASS")
    exit(0)
} else {
    print("\n\(failures) FAILURE(S)")
    exit(1)
}
