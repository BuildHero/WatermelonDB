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
//
// Exit code 0 = all pass; non-zero = failure (CI-usable).

import Foundation

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

private func tempDBPath() -> String {
    let name = "wmdb-5606-\(UUID().uuidString).db"
    return (NSTemporaryDirectory() as NSString).appendingPathComponent(name)
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

print("MOBILE-5606 writer-serialization tests")
test1_standaloneBlocksWhileTransactionHeld()
test2a_standaloneSurvivesConcurrentRollback()
test2b_bareExecuteLostOnConcurrentRollback()

if failures == 0 {
    print("\nALL PASS")
    exit(0)
} else {
    print("\n\(failures) FAILURE(S)")
    exit(1)
}
