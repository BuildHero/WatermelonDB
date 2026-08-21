package com.nozbe.watermelondb

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.runner.RunWith
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

@RunWith(AndroidJUnit4::class)
class DatabaseTest {
    private val context: Context = ApplicationProvider.getApplicationContext()
    private var db: Database? = null
    private val extraDbs = mutableListOf<Database>()

    private fun makeDatabase(): Database {
        val database = Database("wmdb-test-${System.nanoTime()}", context)
        db = database
        return database
    }

    private fun makeDatabaseWithName(name: String): Database {
        val database = Database(name, context)
        extraDbs.add(database)
        return database
    }

    @After
    fun tearDown() {
        db?.close()
        db = null
        extraDbs.forEach { it.close() }
        extraDbs.clear()
    }

    @Test
    fun readOnlyQueryRoutesToReader() {
        val database = makeDatabase()
        assertTrue(database._test_isReadOnlyQuery("select 1"))
        assertEquals("reader", database._test_readDatabaseIdentity("select 1"))
    }

    @Test
    fun writeQueryRoutesToWriter() {
        val database = makeDatabase()
        assertFalse(database._test_isReadOnlyQuery("update foo set bar = 1"))
        assertEquals("writer", database._test_readDatabaseIdentity("update foo set bar = 1"))
    }

    @Test
    fun transactionForcesWriter() {
        val database = makeDatabase()
        database.transaction {
            assertEquals("writer", database._test_readDatabaseIdentity("select 1"))
        }
    }

    @Test
    fun readerIsQueryOnly() {
        val database = makeDatabase()
        assertEquals(1, database._test_readerQueryOnlyValue())
    }

    @Test
    fun readOnlyPrefixes() {
        val database = makeDatabase()
        assertTrue(database._test_isReadOnlyQuery("with cte as (select 1) select * from cte"))
        assertTrue(database._test_isReadOnlyQuery("explain select 1"))
    }

    // -- referencesTemporaryTable detection --

    @Test
    fun detectsTempDotPrefix() {
        val database = makeDatabase()
        assertTrue(database._test_referencesTemporaryTable("SELECT * FROM temp.product_search"))
        assertTrue(database._test_referencesTemporaryTable("select * from TEMP.product_search"))
        assertTrue(database._test_referencesTemporaryTable("CREATE VIRTUAL TABLE IF NOT EXISTS temp.product_search USING fts5(id)"))
        assertTrue(database._test_referencesTemporaryTable("INSERT OR REPLACE INTO temp.product_search(rowid, id) SELECT rowid, id FROM products"))
    }

    @Test
    fun detectsSqliteTempMaster() {
        val database = makeDatabase()
        assertTrue(database._test_referencesTemporaryTable("SELECT name FROM sqlite_temp_master WHERE type='table' AND name='product_search'"))
        assertTrue(database._test_referencesTemporaryTable("SELECT name FROM SQLITE_TEMP_MASTER WHERE type='table'"))
    }

    @Test
    fun doesNotFalsePositiveOnRegularQueries() {
        val database = makeDatabase()
        assertFalse(database._test_referencesTemporaryTable("SELECT * FROM products"))
        assertFalse(database._test_referencesTemporaryTable("SELECT * FROM products WHERE name = 'temporary'"))
        assertFalse(database._test_referencesTemporaryTable("WITH cte AS (SELECT 1) SELECT * FROM cte"))
        assertFalse(database._test_referencesTemporaryTable("UPDATE products SET name = 'test'"))
    }

    // -- readDatabase routing with temp tables --

    @Test
    fun tempDotSelectRoutesToWriter() {
        val database = makeDatabase()
        assertEquals("writer", database._test_readDatabaseIdentity(
            "SELECT * FROM temp.product_search('test')"
        ))
    }

    @Test
    fun withCteTempRoutesToWriter() {
        val database = makeDatabase()
        // WITH queries referencing temp tables should go to writer
        assertEquals("writer", database._test_readDatabaseIdentity(
            """
            WITH filtered_search AS (
                SELECT id FROM temp.product_search(?)
            )
            SELECT * FROM filtered_search
            """.trimIndent()
        ))
    }

    @Test
    fun sqliteTempMasterRoutesToWriter() {
        val database = makeDatabase()
        assertEquals("writer", database._test_readDatabaseIdentity(
            "SELECT name FROM sqlite_temp_master WHERE type='table' AND name='product_search'"
        ))
    }

    @Test
    fun regularSelectStillRoutesToReader() {
        val database = makeDatabase()
        assertEquals("reader", database._test_readDatabaseIdentity("SELECT * FROM products"))
        assertEquals("reader", database._test_readDatabaseIdentity(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ))
    }

    @Test
    fun tempSubstringInStringLiteralStillRoutesToWriter() {
        val database = makeDatabase()
        // Known false positive: "temp." appearing in a string literal or as a table alias
        // routes to writer. This is a safe fallback (just reduces read parallelism).
        assertEquals("writer", database._test_readDatabaseIdentity(
            "SELECT * FROM products WHERE name = 'temp.file'"
        ))
    }

    // -- SQLite connection contention (reproduces HeadlessJS dual-connection scenario) --

    /**
     * Reproduces the production SQLiteDatabaseLockedException:
     *
     * Two separate Database instances open the same SQLite file (simulating the
     * main app thread and a HeadlessJS background sync thread on Android).
     * One holds a long write transaction while the other tries to write.
     *
     * Without openWithRetry, the second connection throws immediately when
     * sqlite3_prepare_v2() hits SQLITE_BUSY during PRAGMA compilation (the
     * busy handler is NOT invoked at the prepare level). openWithRetry retries
     * the entire open+PRAGMA block at the application level for up to 5s.
     *
     * The hold time must exceed requery's built-in default (~2.5s) to reproduce
     * the production scenario where large sync transactions hold locks longer.
     */
    @Test(timeout = 30000)
    fun concurrentWritersWithBusyTimeoutDoNotThrow() {
        val sharedName = "wmdb-contention-${System.nanoTime()}"

        // Two separate Database instances on the same file — same as HeadlessJS scenario
        val db1 = makeDatabaseWithName(sharedName)
        val db2 = makeDatabaseWithName(sharedName)

        // Setup: create table via db1
        db1.execute("CREATE TABLE IF NOT EXISTS test_contention (id TEXT PRIMARY KEY, value TEXT)")

        val writerStarted = CountDownLatch(1)
        val writerDone = CountDownLatch(1)
        val db2Error = AtomicReference<Throwable?>(null)
        val db2Done = CountDownLatch(1)

        // Thread 1: hold a write transaction long enough to exceed any built-in
        // busy timeout in the requery SQLite wrapper (~2.5s). In production, sync
        // transactions routinely hold locks for 5-10+ seconds on large tenants.
        val writerThread = Thread {
            try {
                db1.transaction {
                    for (i in 1..500) {
                        db1.execute(
                            "INSERT OR REPLACE INTO test_contention (id, value) VALUES (?, ?)",
                            arrayOf("bg-sync-$i", "background-data-$i")
                        )
                    }
                    writerStarted.countDown()
                    Thread.sleep(4000)
                }
            } finally {
                writerDone.countDown()
            }
        }

        // Thread 2: try to write while thread 1 holds the lock (simulates foreground user action)
        val readerThread = Thread {
            try {
                writerStarted.await(10, TimeUnit.SECONDS)
                Thread.sleep(50)

                // Without openWithRetry this throws SQLiteDatabaseLockedException
                // once requery's built-in timeout (~2.5s) is exhausted.
                db2.transaction {
                    db2.execute(
                        "INSERT OR REPLACE INTO test_contention (id, value) VALUES (?, ?)",
                        arrayOf("fg-action-1", "foreground-data")
                    )
                }
            } catch (e: Throwable) {
                db2Error.set(e)
            } finally {
                db2Done.countDown()
            }
        }

        writerThread.start()
        readerThread.start()

        assertTrue("Writer thread timed out", writerDone.await(20, TimeUnit.SECONDS))
        assertTrue("Reader thread timed out", db2Done.await(20, TimeUnit.SECONDS))

        // openWithRetry retries until db1 commits — no error
        assertNull(
            "Expected no error with openWithRetry but got: ${db2Error.get()?.message}",
            db2Error.get()
        )

        // Verify both writes landed
        val cursor = db1.rawQuery(
            "SELECT COUNT(*) as count FROM test_contention",
            emptyArray()
        )
        cursor.use {
            it.moveToFirst()
            assertEquals(501, it.getInt(it.getColumnIndex("count")))
        }
    }

    /**
     * Verifies that a foreground read doesn't throw while a background write
     * transaction is in progress. This is the most common production scenario:
     * user taps a visit while background sync is writing.
     */
    @Test
    fun concurrentReadDuringWriteDoesNotThrow() {
        val sharedName = "wmdb-read-contention-${System.nanoTime()}"

        val db1 = makeDatabaseWithName(sharedName)
        val db2 = makeDatabaseWithName(sharedName)

        // Setup
        db1.execute("CREATE TABLE IF NOT EXISTS test_reads (id TEXT PRIMARY KEY, value TEXT)")
        db1.execute(
            "INSERT INTO test_reads (id, value) VALUES (?, ?)",
            arrayOf("existing-1", "data")
        )

        val writerStarted = CountDownLatch(1)
        val writerDone = CountDownLatch(1)
        val readerError = AtomicReference<Throwable?>(null)
        val readerDone = CountDownLatch(1)

        // Thread 1: long write transaction (background sync)
        val writerThread = Thread {
            try {
                db1.transaction {
                    for (i in 1..100) {
                        db1.execute(
                            "INSERT OR REPLACE INTO test_reads (id, value) VALUES (?, ?)",
                            arrayOf("sync-$i", "sync-data-$i")
                        )
                    }
                    writerStarted.countDown()
                    Thread.sleep(500)
                }
            } finally {
                writerDone.countDown()
            }
        }

        // Thread 2: read while write is in progress (user tapping a visit)
        val readerThread = Thread {
            try {
                writerStarted.await(5, TimeUnit.SECONDS)
                Thread.sleep(50)

                // Read via the reader connection — should work with WAL + busy_timeout
                val cursor = db2.rawQuery(
                    "SELECT value FROM test_reads WHERE id = ?",
                    arrayOf("existing-1")
                )
                cursor.use {
                    it.moveToFirst()
                    assertEquals("data", it.getString(0))
                }
            } catch (e: Throwable) {
                readerError.set(e)
            } finally {
                readerDone.countDown()
            }
        }

        writerThread.start()
        readerThread.start()

        assertTrue("Writer timed out", writerDone.await(10, TimeUnit.SECONDS))
        assertTrue("Reader timed out", readerDone.await(10, TimeUnit.SECONDS))

        assertNull(
            "Read during write should not throw: ${readerError.get()?.message}",
            readerError.get()
        )
    }

    /**
     * MOBILE-6492 fix verification. Previously (see git history:
     * `alreadyOpenConnectionThrowsOnBusyWithNoRetry`), this reproduced a gap left by
     * MOBILE-5065 / openWithRetry: retry-with-backoff only wrapped the ONE-TIME lazy
     * `writerDb`/`readerDb` initializer block (open + PRAGMA setup). Once a connection
     * had already completed that lazy init, every subsequent `execute()` /
     * `transaction()` call went straight to the raw `SQLiteDatabase` object with zero
     * retry and no `busy_timeout` — so a SECOND writer that had been open and idle for a
     * while got no protection at all if it collided with another writer's long
     * transaction, unlike a freshly-opening connection which openWithRetry covered.
     *
     * This was the production shape behind SIP-16015 / MOBILE-6492 (Android
     * `SQLiteDatabaseLockedException` observed from `sync-coordinator`, `syncDatabase`,
     * `VisitActions.js`, and `jobRepository.ts` — all long-lived, already-initialized
     * connections, not fresh connection opens).
     *
     * Fix: `writerDb`/`readerDb` now set `PRAGMA busy_timeout=5000` (matching iOS's
     * `Database.swift#setWalMode`), so SQLite's own native busy-handler waits instead of
     * failing immediately on an already-open connection's ordinary write.
     */
    @Test(timeout = 30000)
    fun alreadyOpenConnectionWaitsOnBusyInsteadOfThrowing() {
        val sharedName = "wmdb-post-init-contention-${System.nanoTime()}"

        val db1 = makeDatabaseWithName(sharedName)
        val db2 = makeDatabaseWithName(sharedName)

        // Force BOTH connections to complete their lazy writerDb init BEFORE the race,
        // simulating long-lived connections that have already been open for a while —
        // NOT the connection-init moment openWithRetry protects.
        db1.execute("CREATE TABLE IF NOT EXISTS test_post_init (id TEXT PRIMARY KEY, value TEXT)")
        db2.execute("CREATE TABLE IF NOT EXISTS test_post_init (id TEXT PRIMARY KEY, value TEXT)")

        val writerStarted = CountDownLatch(1)
        val writerDone = CountDownLatch(1)
        val db2Error = AtomicReference<Throwable?>(null)
        val db2Done = CountDownLatch(1)

        // Thread 1: hold a write transaction well past requery's ~2.5s built-in busy
        // timeout — same hold duration as concurrentWritersWithBusyTimeoutDoNotThrow,
        // which openWithRetry successfully protects against for a FRESH connection.
        val writerThread = Thread {
            try {
                db1.transaction {
                    db1.execute(
                        "INSERT OR REPLACE INTO test_post_init (id, value) VALUES (?, ?)",
                        arrayOf("bg-sync-1", "background-data")
                    )
                    writerStarted.countDown()
                    Thread.sleep(4000)
                }
            } finally {
                writerDone.countDown()
            }
        }

        // Thread 2: db2 is ALREADY initialized (see above) — this is an ordinary
        // post-init write, not a connection open, so it is NOT wrapped in openWithRetry.
        val secondWriterThread = Thread {
            try {
                writerStarted.await(10, TimeUnit.SECONDS)
                Thread.sleep(50)
                db2.transaction {
                    db2.execute(
                        "INSERT OR REPLACE INTO test_post_init (id, value) VALUES (?, ?)",
                        arrayOf("fg-action-1", "foreground-data")
                    )
                }
            } catch (e: Throwable) {
                db2Error.set(e)
            } finally {
                db2Done.countDown()
            }
        }

        writerThread.start()
        secondWriterThread.start()

        assertTrue("Writer thread timed out", writerDone.await(20, TimeUnit.SECONDS))
        assertTrue("Second writer thread timed out", db2Done.await(20, TimeUnit.SECONDS))

        // FIXED: with busy_timeout=5000 set on both writerDb and readerDb, db2's
        // already-open connection now waits on SQLite's native busy-handler instead of
        // throwing immediately — same protection concurrentWritersWithBusyTimeoutDoNotThrow
        // already verified for a freshly-opening connection via openWithRetry.
        assertNull(
            "Expected no error now that busy_timeout protects already-open connections, but got: ${db2Error.get()?.message}",
            db2Error.get()
        )
    }

    /**
     * MOBILE-6492 Tier 2 MRE. Confirmed via production log analysis (Jonathan
     * DiCamillo, SIP-16015): the real incident was a ~9-minute continuous lock that
     * survived a full JS re-init — not transient contention. A fixed `busy_timeout`
     * cannot cover a holder that never releases within that window; it only delays
     * the eventual throw. This characterizes that gap: a second, independent writer
     * connection holds a transaction well past `busy_timeout` (Tier 1 fix), and an
     * ordinary write from a separate already-open connection still throws once the
     * timeout is exhausted.
     *
     * Root cause (confirmed by reading the fork's native sync-apply path):
     * `JSIAndroidBridgeModule.cpp`'s `ApplyCallback` writes to SQLite via
     * `Database.kt#acquireSqliteConnection()` + the shared `applySyncPayload()`
     * free function (`native/shared/SyncApplyEngine.cpp`), entirely OUTSIDE
     * `Database.kt#transaction()` and with zero serialization against it — unlike
     * iOS, whose `Database.swift#writerTransactionSemaphore` wraps both
     * `inTransaction()` and `JSISwiftWrapperModule.mm`'s equivalent sync-apply
     * callback. Android's `JSIAndroidBridgeModule` has no semaphore/lock equivalent
     * at all (confirmed via grep — zero synchronization primitives protect the
     * writer there).
     *
     * This test models the two genuinely-separate-connection shape `applySyncPayload`
     * produces (see `concurrentWritersWithBusyTimeoutDoNotThrow`'s docstring for why
     * two distinct `Database` instances on the same file reproduce a real SQLite
     * BUSY, unlike two threads sharing one connection pool, which just serializes at
     * the Java level).
     *
     * Fix (Tier 2, implemented): a per-database-file writer semaphore
     * (`Database.kt#writerTransactionSemaphore`, shared across every `Database`
     * instance on the same path via a companion-object map — the Kotlin-level
     * counterpart of what `DatabaseBridge.kt`'s native accessors will expose to the
     * `ApplyCallback`), acquired/released around `transaction()` — mirroring iOS's
     * `writerTransactionSemaphore` exactly. `db2`'s write now waits for `db1` to
     * release instead of throwing, and only completes after `db1`'s hold duration has
     * elapsed — verified below by asserting `db2Error` is null and its elapsed time is
     * at least `holdDurationMs`.
     */
    @Test(timeout = 30000)
    fun orphanedHolderBlocksSecondWriterUntilReleaseInsteadOfThrowing() {
        val sharedName = "wmdb-orphaned-holder-${System.nanoTime()}"
        val holdDurationMs = 7000L // comfortably past the 5000ms busy_timeout

        val db1 = makeDatabaseWithName(sharedName)
        val db2 = makeDatabaseWithName(sharedName)

        // Force both connections to complete their lazy writerDb init before the race
        // (already-open connections, not the connection-init case openWithRetry covers).
        db1.execute("CREATE TABLE IF NOT EXISTS test_orphaned_holder (id TEXT PRIMARY KEY, value TEXT)")
        db2.execute("CREATE TABLE IF NOT EXISTS test_orphaned_holder (id TEXT PRIMARY KEY, value TEXT)")

        val writerStarted = CountDownLatch(1)
        val writerDone = CountDownLatch(1)
        val db2Error = AtomicReference<Throwable?>(null)
        val db2Done = CountDownLatch(1)
        val db2StartTime = AtomicReference<Long?>(null)
        val db2EndTime = AtomicReference<Long?>(null)

        // Thread 1: simulates the native sync-apply path's transaction, held well past
        // busy_timeout — standing in for a background-sync writer that never releases
        // within a reasonable window (WorkManager kill / unsafe ON_START cancel).
        val writerThread = Thread {
            try {
                db1.transaction {
                    db1.execute(
                        "INSERT OR REPLACE INTO test_orphaned_holder (id, value) VALUES (?, ?)",
                        arrayOf("bg-sync-1", "background-data")
                    )
                    writerStarted.countDown()
                    Thread.sleep(holdDurationMs)
                }
            } finally {
                writerDone.countDown()
            }
        }

        // Thread 2: an ordinary already-open-connection write, simulating a foreground
        // visit-action write colliding with the orphaned background writer.
        val secondWriterThread = Thread {
            try {
                writerStarted.await(10, TimeUnit.SECONDS)
                Thread.sleep(50)
                db2StartTime.set(System.currentTimeMillis())
                db2.transaction {
                    db2.execute(
                        "INSERT OR REPLACE INTO test_orphaned_holder (id, value) VALUES (?, ?)",
                        arrayOf("fg-action-1", "foreground-data")
                    )
                }
                db2EndTime.set(System.currentTimeMillis())
            } catch (e: Throwable) {
                db2Error.set(e)
            } finally {
                db2Done.countDown()
            }
        }

        writerThread.start()
        secondWriterThread.start()

        assertTrue("Writer thread timed out", writerDone.await(20, TimeUnit.SECONDS))
        assertTrue("Second writer thread timed out", db2Done.await(20, TimeUnit.SECONDS))

        // FIXED (Tier 2): the writer semaphore makes db2 wait for db1 to release
        // rather than racing SQLite's busy_timeout — no exception, regardless of how
        // long db1 holds the lock.
        assertNull(
            "Expected no error now that the writer semaphore serializes db2 against " +
                "db1's transaction, but got: ${db2Error.get()?.message}",
            db2Error.get()
        )

        // And it must have genuinely WAITED for the semaphore (not raced SQLite and
        // gotten lucky) — db2's write should only complete at or after db1's release.
        val elapsedMs = (db2EndTime.get() ?: 0L) - (db2StartTime.get() ?: 0L)
        assertTrue(
            "Expected db2 to wait at least ${holdDurationMs}ms for db1's semaphore release, " +
                "but only waited ${elapsedMs}ms",
            elapsedMs >= holdDurationMs - 200 // small tolerance for scheduling jitter
        )
    }

    /**
     * MOBILE-6492 — the case the incident actually was, and the one
     * `orphanedHolderBlocksSecondWriterUntilReleaseInsteadOfThrowing` above does NOT
     * cover: a holder that never releases at all.
     *
     * That test sleeps 7s and then releases, so it only proves a *bounded* hold no
     * longer throws. The reported failure was a native sync-apply transaction orphaned
     * by the ON_START foreground-cancel path (which resets bookkeeping without rolling
     * back) — the lock was held for 9 minutes and survived a full JS re-init. Against a
     * holder like that, an unbounded `Semaphore.acquire()` never returns, converting a
     * visible SQLiteDatabaseLockedException into a permanent silent stall of every
     * writer on the file.
     *
     * So the invariant under test is liveness, not success: a writer must always come
     * back — with or without an error — inside a bound derived from
     * `writerSemaphoreTimeoutMs`. It must never hang.
     */
    @Test
    fun wedgedHolderDoesNotBlockSecondWriterForever() {
        val sharedName = "wmdb-wedged-holder-${System.nanoTime()}"
        val timeoutMs = 2000L

        val originalTimeout = Database.writerSemaphoreTimeoutMs
        Database.writerSemaphoreTimeoutMs = timeoutMs
        try {
            val db1 = makeDatabaseWithName(sharedName)
            val db2 = makeDatabaseWithName(sharedName)
            db1.execute(
                "CREATE TABLE IF NOT EXISTS test_wedged (id TEXT PRIMARY KEY, value TEXT)"
            )

            // Take the permit through the same accessor the native sync-apply path uses
            // and deliberately NEVER release it — standing in for the orphaned native
            // apply transaction. No matching release anywhere in this test.
            assertTrue(
                "Precondition: the wedging acquire should succeed on an idle semaphore",
                db1.acquireWriterTransactionSemaphore()
            )

            val db2Done = CountDownLatch(1)
            val db2Error = AtomicReference<Throwable?>(null)
            val startMs = System.currentTimeMillis()

            Thread {
                try {
                    db2.transaction {
                        db2.execute(
                            "INSERT OR REPLACE INTO test_wedged (id, value) VALUES (?, ?)",
                            arrayOf("w1", "written-despite-wedge")
                        )
                    }
                } catch (t: Throwable) {
                    db2Error.set(t)
                } finally {
                    db2Done.countDown()
                }
            }.start()

            // The assertion that matters: it RETURNS. Generous ceiling so this fails on
            // a genuine hang, not on emulator jitter. Pre-fix this times out here.
            assertTrue(
                "Second writer never returned — it is blocked forever behind a holder " +
                    "that never releases (unbounded acquire regression)",
                db2Done.await(timeoutMs * 5, TimeUnit.MILLISECONDS)
            )

            // It must have actually waited for the bound rather than sailing past a
            // semaphore that was not really held.
            val elapsedMs = System.currentTimeMillis() - startMs
            assertTrue(
                "Expected the writer to wait out the ${timeoutMs}ms bound, waited ${elapsedMs}ms",
                elapsedMs >= timeoutMs - 200
            )

            // Whether the write itself succeeded is deliberately NOT asserted: past the
            // bound we proceed unserialized and busy_timeout arbitrates, so either
            // outcome is correct. Only hanging is a failure.
        } finally {
            Database.writerSemaphoreTimeoutMs = originalTimeout
        }
    }

    /**
     * MOBILE-6492 — the writer semaphore must be reentrant.
     *
     * `Database.transaction()` nests in production. Both `DatabaseDriver.setUpSchema`
     * and `.migrate` do:
     *
     *     database.transaction {                        // takes the only permit
     *         database.unsafeExecuteStatements(sql)     // transacts AGAIN, same thread
     *         database.userVersion = ...
     *     }
     *
     * and `unsafeExecuteStatements` is itself `transaction { ... }`. `Semaphore(1)` is
     * not reentrant, so a non-reentrant implementation makes the inner frame wait on the
     * permit its own thread holds — a self-deadlock with no counterparty to release it.
     * Those two call sites are first-launch schema setup and every app upgrade carrying
     * a migration, so this must never regress.
     *
     * The existing suite cannot catch it: every other test drives `Database` directly
     * and never goes through `DatabaseDriver`, so neither nesting path is exercised.
     * This test reproduces the nesting shape against the same public API.
     */
    @Test
    fun nestedTransactionDoesNotSelfDeadlock() {
        val timeoutMs = 1500L
        val originalTimeout = Database.writerSemaphoreTimeoutMs
        // Deliberately short: if reentrancy regresses, the inner frame blocks and this
        // fails fast on the latch below instead of stalling the whole suite.
        Database.writerSemaphoreTimeoutMs = timeoutMs
        try {
            val database = makeDatabase()
            database.execute(
                "CREATE TABLE IF NOT EXISTS test_nested (id TEXT PRIMARY KEY, value TEXT)"
            )

            val done = CountDownLatch(1)
            val failure = AtomicReference<Throwable?>(null)
            val startMs = System.currentTimeMillis()

            Thread {
                try {
                    // Mirrors DatabaseDriver.setUpSchema: an outer transaction wrapping
                    // unsafeExecuteStatements, which opens its own inner transaction.
                    database.transaction {
                        database.unsafeExecuteStatements(
                            "INSERT OR REPLACE INTO test_nested (id, value) VALUES ('n1', 'inner');"
                        )
                        database.userVersion = 42
                    }
                } catch (t: Throwable) {
                    failure.set(t)
                } finally {
                    done.countDown()
                }
            }.start()

            assertTrue(
                "Nested transaction never completed — the inner frame is deadlocked on " +
                    "the permit its own thread holds (writer semaphore lost reentrancy)",
                done.await(timeoutMs * 4, TimeUnit.MILLISECONDS)
            )
            assertNull("Nested transaction threw: ${failure.get()?.message}", failure.get())

            // It must not have merely *survived* by timing out the inner acquire — that
            // would still stall every schema setup and migration for the bound.
            val elapsedMs = System.currentTimeMillis() - startMs
            assertTrue(
                "Nested transaction took ${elapsedMs}ms — it waited out the semaphore " +
                    "bound instead of re-entering, so reentrancy is not working",
                elapsedMs < timeoutMs
            )

            // The nested write really landed, and the permit was fully surrendered.
            assertEquals(42, database.userVersion)
            assertEquals(
                "Permit must be back to exactly 1 after the outermost frame exits",
                1,
                database.availableWriterPermitsForTest()
            )
        } finally {
            Database.writerSemaphoreTimeoutMs = originalTimeout
        }
    }

    /**
     * MOBILE-6492 — the writer-contention counters must capture the two facts a field
     * investigation cannot otherwise get.
     *
     * The on-device issue report is assembled from the JS logger and captures no
     * logcat, so `Database`'s `Log.w` on a timeout never reaches it. A sweep of 1,198
     * production reports could therefore establish which call sites were *failing*
     * (sync-coordinator, syncDatabase, JobTaskSync) but never which writer was
     * *holding*, nor for how long — which is exactly what distinguishes a wedged
     * holder from ordinary transient contention.
     *
     * So this pins both: the holder named at timeout, and `maxWaitMs` as a real
     * measurement of how long another writer held the file.
     */
    @Test
    fun writerContentionStatsRecordHolderAndWaitDuration() {
        val sharedName = "wmdb-contention-stats-${System.nanoTime()}"
        val timeoutMs = 800L
        val holdMs = 400L

        val originalTimeout = Database.writerSemaphoreTimeoutMs
        Database.writerSemaphoreTimeoutMs = timeoutMs
        try {
            val db1 = makeDatabaseWithName(sharedName)
            val db2 = makeDatabaseWithName(sharedName)

            // 1. A timeout must name the holder that caused it.
            assertTrue(
                "Precondition: first acquire on an idle semaphore",
                db1.acquireWriterTransactionSemaphore("test-holder")
            )

            val timedOutResult = AtomicReference<Boolean?>(null)
            val timeoutDone = CountDownLatch(1)
            Thread {
                try {
                    timedOutResult.set(db2.acquireWriterTransactionSemaphore())
                } finally {
                    timeoutDone.countDown()
                }
            }.start()

            assertTrue(
                "Contender never returned from acquire",
                timeoutDone.await(timeoutMs * 6, TimeUnit.MILLISECONDS)
            )
            assertFalse("Contender should have timed out", timedOutResult.get()!!)

            val afterTimeout = db1.writerContentionSnapshot()
            assertEquals("One timeout should be recorded", 1L, afterTimeout["timeouts"])
            assertEquals(
                "The timeout must name the writer that was holding the permit",
                "test-holder",
                afterTimeout["lastTimeoutHolder"]
            )

            db1.releaseWriterTransactionSemaphore()

            // 2. A contended-but-successful acquire must record how long it waited —
            //    the proxy for how long the other writer actually held the file.
            assertTrue(
                "Re-acquire after release",
                db1.acquireWriterTransactionSemaphore("holder-2")
            )

            val waitedResult = AtomicReference<Boolean?>(null)
            val waitDone = CountDownLatch(1)
            Thread {
                try {
                    val got = db2.acquireWriterTransactionSemaphore()
                    waitedResult.set(got)
                    // Release on the acquiring thread: reentrancy depth is per-thread,
                    // so a cross-thread release would find depth 0 and correctly refuse.
                    if (got) db2.releaseWriterTransactionSemaphore()
                } finally {
                    waitDone.countDown()
                }
            }.start()

            Thread.sleep(holdMs)
            db1.releaseWriterTransactionSemaphore()

            assertTrue(
                "Waiting writer never completed",
                waitDone.await(timeoutMs * 6, TimeUnit.MILLISECONDS)
            )
            assertTrue(
                "The waiting writer should have succeeded once the hold ended",
                waitedResult.get()!!
            )

            val afterWait = db2.writerContentionSnapshot()
            val maxWaitMs = afterWait["maxWaitMs"] as Long
            assertTrue(
                "maxWaitMs should reflect the ~${holdMs}ms hold, got ${maxWaitMs}ms",
                maxWaitMs >= holdMs - 150
            )
            assertTrue(
                "The contended acquire should have been counted, got ${afterWait["contendedAcquires"]}",
                (afterWait["contendedAcquires"] as Long) >= 1L
            )
            assertEquals(
                "The reported bound should be the active timeout",
                timeoutMs,
                afterWait["timeoutBoundMs"]
            )
        } finally {
            Database.writerSemaphoreTimeoutMs = originalTimeout
        }
    }

    /**
     * MOBILE-6492 — guards the silent-no-op bug: releasing a binary Semaphore without a
     * matching acquire *raises* its permit count (1 -> 2), after which two writers hold
     * it simultaneously and Tier 2's mutual exclusion is gone for the life of the
     * process, with every existing test still green.
     *
     * The reachable path was the JNI boundary: `acquireWriterSemaphore` used to return
     * void, so a failure (no env/class, missing method, or a Java-side throw cleared by
     * ExceptionCheck) was indistinguishable from success and the caller released
     * anyway. Acquire now reports success and callers release only on true; this pins
     * the invariant that a failed acquire followed by no release cannot inflate permits.
     */
    @Test
    fun failedAcquireDoesNotInflatePermits() {
        val sharedName = "wmdb-permit-inflation-${System.nanoTime()}"
        val timeoutMs = 500L

        val originalTimeout = Database.writerSemaphoreTimeoutMs
        Database.writerSemaphoreTimeoutMs = timeoutMs
        try {
            val db1 = makeDatabaseWithName(sharedName)
            val db2 = makeDatabaseWithName(sharedName)

            assertTrue("First acquire should succeed", db1.acquireWriterTransactionSemaphore())
            assertEquals(
                "Binary semaphore should have no permits left while held",
                0,
                db1.availableWriterPermitsForTest()
            )

            // The contending acquire MUST run on another thread. Reentrancy is keyed on
            // (thread, database file), so a second acquire on *this* thread would
            // correctly re-enter and return true — that is the nesting case, not the
            // contention case. Only a different thread actually contends for the permit.
            val contenderResult = AtomicReference<Boolean?>(null)
            val contenderDone = CountDownLatch(1)
            Thread {
                try {
                    contenderResult.set(db2.acquireWriterTransactionSemaphore())
                } finally {
                    contenderDone.countDown()
                }
            }.start()

            assertTrue(
                "Contending thread never returned from acquire",
                contenderDone.await(timeoutMs * 6, TimeUnit.MILLISECONDS)
            )
            assertFalse(
                "A contending thread's acquire must time out and report false while " +
                    "another thread holds the permit",
                contenderResult.get()!!
            )

            // The failed acquirer correctly does not release. Only the real holder does.
            db1.releaseWriterTransactionSemaphore()

            assertEquals(
                "Permit count must return to exactly 1 — anything higher means an " +
                    "unmatched release inflated the semaphore and mutual exclusion is void",
                1,
                db1.availableWriterPermitsForTest()
            )

            // And exclusion still holds afterwards: one permit, taken by one acquirer.
            assertTrue(
                "Acquire should work after a clean release",
                db1.acquireWriterTransactionSemaphore()
            )
            assertEquals(
                "Still a binary semaphore after a full acquire/release cycle",
                0,
                db1.availableWriterPermitsForTest()
            )
            db1.releaseWriterTransactionSemaphore()
        } finally {
            Database.writerSemaphoreTimeoutMs = originalTimeout
        }
    }
}
