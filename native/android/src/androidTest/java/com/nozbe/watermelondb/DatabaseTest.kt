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
}
