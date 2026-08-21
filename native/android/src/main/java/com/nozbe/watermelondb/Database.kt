package com.nozbe.watermelondb

import android.content.Context
import android.database.Cursor
import androidx.core.os.CancellationSignal
import io.requery.android.database.sqlite.SQLiteConnection
import io.requery.android.database.sqlite.SQLiteConnectionPool.CONNECTION_FLAG_PRIMARY_CONNECTION_AFFINITY
import io.requery.android.database.sqlite.SQLiteDatabase
import io.requery.android.database.sqlite.SQLiteSession
import io.requery.android.database.sqlite.SQLiteUpdateHook
import android.database.sqlite.SQLiteFullException
import android.util.Log
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class Database(private val name: String, private val context: Context) {

    private val databasePath: String = resolveDatabasePath()
    private val transactionDepth = ThreadLocal<Int>()

    companion object {
        // MOBILE-6492 (Tier 2): one binary semaphore per underlying database file,
        // shared across every `Database` instance (and, via DatabaseBridge's JNI
        // accessors, the native sync-apply path) pointing at that same path. This is
        // the Android port of iOS's `Database.swift#writerTransactionSemaphore` —
        // Android's native sync-apply callback (`JSIAndroidBridgeModule.cpp`) writes
        // to this same connection via `acquireSqliteConnection()` below, entirely
        // outside `transaction()`'s own bookkeeping, so the semaphore must live at a
        // scope both paths can reach, keyed by the one thing they agree on: the file.
        private val writerTransactionSemaphores = ConcurrentHashMap<String, Semaphore>()
        private val writerHolders = ConcurrentHashMap<String, String>()

        internal fun writerTransactionSemaphoreFor(path: String): Semaphore =
            writerTransactionSemaphores.getOrPut(path) { Semaphore(1) }

        // MOBILE-6492: how long a writer waits for the semaphore before giving up.
        // Sized from the incident analysis: a real sync transaction holds the writer
        // for 5-10s, so 30s leaves ample headroom for the legitimate case, while
        // being far below the multi-minute wedge this bound exists to escape.
        //
        // A bound is mandatory, not defensive. `acquire()` never returns if a holder
        // never releases — which is precisely the reported failure (a native
        // sync-apply transaction orphaned by the ON_START foreground-cancel path,
        // which resets bookkeeping without rolling back). Unbounded, that turns a
        // visible `SQLiteDatabaseLockedException` into a silent, permanent stall of
        // every writer on the file.
        //
        // `var` only so instrumented tests can shrink it; nothing in production
        // reassigns it.
        internal var writerSemaphoreTimeoutMs = 30_000L

        // MOBILE-6492: reentrancy bookkeeping for the writer semaphore, keyed by
        // database file, per thread.
        //
        // `transaction()` genuinely nests in production: DatabaseDriver.setUpSchema and
        // .migrate both wrap `database.unsafeExecuteStatements(...)` inside their own
        // `database.transaction { }`, and unsafeExecuteStatements transacts again.
        // Semaphore(1) is NOT reentrant, so without this the inner frame waits on the
        // permit its own thread is holding — an unbreakable self-deadlock on the two
        // paths that must never hang: first-launch schema setup, and every migration.
        //
        // Per-thread is sound because all three writer paths acquire and release on one
        // thread: JS transactions on the caller's thread, the native sync-apply inside a
        // single OkHttp-callback invocation, and slice import on the single work-queue
        // thread (SlicePlatformAndroid's gWorkThreadId) for both begin and commit.
        private val writerDepths = object : ThreadLocal<MutableMap<String, Int>>() {
            override fun initialValue(): MutableMap<String, Int> = HashMap()
        }

        // MOBILE-6492: writer-contention counters, per database file.
        //
        // These exist because the on-device issue report — the only artifact available
        // for a field device — is written by the JS logger (`app/utils/logger`), and
        // nothing anywhere captures logcat. So the Log.w this class emits on a timeout
        // is invisible in exactly the artifact an investigation reads. A blast-radius
        // sweep of 1,198 reports could establish who was failing but never who was
        // *holding*, nor for how long.
        //
        // `maxWaitMs` is the load-bearing one: a successful acquire that waited N ms
        // means some other writer held the file for ~N ms, which is a direct field
        // measurement of real hold durations. Sustained values near
        // `writerSemaphoreTimeoutMs` mean a genuinely wedged holder; values in the
        // single-digit seconds mean ordinary transient contention.
        //
        // Read via DatabaseBridge.getWriterContentionStats so the app can fold them
        // into its issue report.
        private val writerStats = ConcurrentHashMap<String, WriterContentionStats>()

        internal fun writerStatsFor(path: String): WriterContentionStats =
            writerStats.getOrPut(path) { WriterContentionStats() }

        // Anything above this is a real wait worth counting; below it is scheduler noise
        // on an uncontended permit.
        private const val CONTENDED_WAIT_THRESHOLD_MS = 50L
    }

    internal class WriterContentionStats {
        val contendedAcquires = AtomicLong(0)
        val timeouts = AtomicLong(0)
        val maxWaitMs = AtomicLong(0)

        @Volatile
        var lastTimeoutHolder: String? = null

        fun recordWait(waitMs: Long) {
            contendedAcquires.incrementAndGet()
            // CAS loop: several writers can finish waiting concurrently, and a plain
            // compare-then-set would let a smaller wait clobber a larger one.
            while (true) {
                val current = maxWaitMs.get()
                if (waitMs <= current || maxWaitMs.compareAndSet(current, waitMs)) return
            }
        }
    }

    private val writerStatsForThisDb: WriterContentionStats
        get() = writerStatsFor(databasePath)

    private val writerTransactionSemaphore: Semaphore
        get() = writerTransactionSemaphoreFor(databasePath)

    internal fun setWriterHolder(name: String) {
        writerHolders[databasePath] = name
    }

    internal fun clearWriterHolder() {
        writerHolders.remove(databasePath)
    }

    internal fun currentWriterHolder(): String? = writerHolders[databasePath]

    // MOBILE-6492: test-only view of the writer semaphore's permit count, so the
    // permit-inflation regression test can assert the invariant (exactly one permit
    // when idle) without widening access to the private database path.
    internal fun availableWriterPermitsForTest(): Int =
        writerTransactionSemaphore.availablePermits()

    // MOBILE-6492 (Tier 2): exposed for DatabaseBridge's JNI accessors, so the native
    // sync-apply path (JSIAndroidBridgeModule.cpp's ApplyCallback) can acquire/release
    // the same semaphore transaction() uses, around its own acquireSqliteConnection()/
    // applySyncPayload()/releaseSQLiteConnection() sequence — mirroring iOS's
    // JSISwiftWrapperModule.mm wait()/signal() around getRawConnection/applySyncPayload.
    //
    // Reentrant, and blocks for at most `writerSemaphoreTimeoutMs`. Returns true if the
    // caller now owns a frame and MUST pair it with releaseWriterTransactionSemaphore()
    // — whether that frame took the permit outright or re-entered one this thread
    // already holds. Returns false only on timeout, in which case the caller MUST NOT
    // release: an unmatched release *raises* a binary Semaphore's permit count and
    // silently voids mutual exclusion for the rest of the process's life.
    fun acquireWriterTransactionSemaphore(holderName: String? = null): Boolean {
        val depths = writerDepths.get()!!
        val depth = depths[databasePath] ?: 0
        if (depth > 0) {
            // Nested frame on the thread that already owns the permit — must not touch
            // the semaphore at all. See the writerDepths note above.
            depths[databasePath] = depth + 1
            return true
        }
        // Capture the holder BEFORE waiting: once we time out, whoever held the permit
        // may already have released and been replaced, so reading it afterwards can
        // name the wrong owner (or nobody).
        val contendedBy = currentWriterHolder()
        val startNs = System.nanoTime()
        val acquired =
            writerTransactionSemaphore.tryAcquire(writerSemaphoreTimeoutMs, TimeUnit.MILLISECONDS)
        val waitedMs = (System.nanoTime() - startNs) / 1_000_000
        val stats = writerStatsForThisDb

        if (!acquired) {
            stats.timeouts.incrementAndGet()
            stats.lastTimeoutHolder = contendedBy ?: "unknown"
            return false
        }
        if (waitedMs >= CONTENDED_WAIT_THRESHOLD_MS) {
            stats.recordWait(waitedMs)
        }

        depths[databasePath] = 1
        if (holderName != null) {
            setWriterHolder(holderName)
        }
        return true
    }

    // MOBILE-6492: snapshot of the writer-contention counters for this database file.
    // Cheap, allocation-light, and safe to call from any thread.
    internal fun writerContentionSnapshot(): Map<String, Any> {
        val stats = writerStatsForThisDb
        return mapOf(
            "contendedAcquires" to stats.contendedAcquires.get(),
            "timeouts" to stats.timeouts.get(),
            "maxWaitMs" to stats.maxWaitMs.get(),
            "timeoutBoundMs" to writerSemaphoreTimeoutMs,
            "lastTimeoutHolder" to (stats.lastTimeoutHolder ?: ""),
            "currentHolder" to (currentWriterHolder() ?: "")
        )
    }

    fun releaseWriterTransactionSemaphore() {
        val depths = writerDepths.get()!!
        when (val depth = depths[databasePath] ?: 0) {
            0 -> Log.w(
                "watermelondb",
                "releaseWriterTransactionSemaphore called without a held permit; " +
                    "ignoring (releasing would inflate the permit count and void " +
                    "writer mutual exclusion for this process)"
            )
            1 -> {
                depths.remove(databasePath)
                clearWriterHolder()
                writerTransactionSemaphore.release()
            }
            else -> depths[databasePath] = depth - 1
        }
    }

    private val writerDb: SQLiteDatabase by lazy {
        openWithRetry {
            SQLiteDatabase.openOrCreateDatabase(databasePath, null).also {
                // Must be first — if another connection holds a lock, all subsequent
                // PRAGMAs (including journal_mode) would throw immediately. Matches
                // iOS's Database.swift#setWalMode (busy_timeout=5000, set first).
                runPragma(it, "PRAGMA busy_timeout=5000")
                runPragma(it, "PRAGMA journal_mode=WAL")
                runPragma(it, "PRAGMA synchronous=NORMAL")  // FULL is too slow, NORMAL is safe with WAL
                runPragma(it, "PRAGMA temp_store=MEMORY")   // Faster temp operations
                runPragma(it, "PRAGMA mmap_size=268435456") // 256MB memory-mapped I/O
            }
        }
    }

    private val readerDb: SQLiteDatabase by lazy {
        // Ensure writer is opened and WAL is enabled before opening the reader.
        writerDb
        if (isInMemoryPath(databasePath)) {
            writerDb
        } else {
            openWithRetry {
                SQLiteDatabase.openDatabase(databasePath, null, SQLiteDatabase.OPEN_READONLY).also {
                    try {
                        runPragma(it, "PRAGMA busy_timeout=5000")
                        runPragma(it, "PRAGMA query_only=1")
                    } catch (_: Exception) {
                        // Best effort; some builds may not allow setting pragmas on read-only connections.
                    }
                }
            }
        }
    }

    private fun runPragma(db: SQLiteDatabase, sql: String) {
        db.rawQuery(sql, null).use { /* pragma executed */ }
    }

    /**
     * Retry a database open + PRAGMA block on SQLiteDatabaseLockedException.
     *
     * sqlite3_prepare_v2() (used by rawQuery to compile PRAGMAs) does NOT
     * invoke the sqlite3 busy handler — it returns SQLITE_BUSY immediately.
     * Additionally, the requery connection pool may run internal PRAGMAs
     * during openOrCreateDatabase() that also fail under lock contention.
     * We must retry the entire open+configure block at the application level.
     *
     * Retries up to 5s with 200ms backoff to match the production scenario
     * where HeadlessJS background sync transactions hold locks for seconds.
     */
    private fun <T> openWithRetry(
        maxWaitMs: Long = 5000,
        intervalMs: Long = 200,
        block: () -> T
    ): T {
        val deadline = System.currentTimeMillis() + maxWaitMs
        var lastException: Exception? = null
        while (System.currentTimeMillis() < deadline) {
            try {
                return block()
            } catch (e: Exception) {
                if (e.message?.contains("database is locked") == true ||
                    e.javaClass.simpleName == "SQLiteDatabaseLockedException") {
                    lastException = e
                    Thread.sleep(intervalMs)
                } else {
                    throw e
                }
            }
        }
        throw lastException ?: IllegalStateException("Database open retry exhausted")
    }

    var userVersion: Int
        get() = writerDb.version
        set(value) {
            writerDb.version = value
        }

    fun acquireSqliteConnection(): Long {
        return acquireSqliteConnection(writerDb)
    }

    fun acquireSqliteReadConnection(): Long {
        return acquireSqliteConnection(readerDb)
    }

    private fun acquireSqliteConnection(db: SQLiteDatabase): Long {
        val getThreadConnectionFlags = db.javaClass.getDeclaredMethod("getThreadDefaultConnectionFlags", Boolean::class.java)

        getThreadConnectionFlags.isAccessible = true

        val flags = getThreadConnectionFlags.invoke(db, false)

        val getThreadSession: Method = db.javaClass.getDeclaredMethod("getThreadSession")

        getThreadSession.isAccessible = true

        val session = getThreadSession.invoke(db) as? SQLiteSession

        val connection = session?.let {
            val acquireConnectionMethod = it.javaClass.getDeclaredMethod(
                "acquireConnection",
                String::class.java,
                Int::class.javaPrimitiveType,
                CancellationSignal::class.java
            )

            acquireConnectionMethod.isAccessible = true

            acquireConnectionMethod.invoke(session, null, flags, null)

            val mConnectionField = it.javaClass.getDeclaredField("mConnection")

            mConnectionField.isAccessible = true

            mConnectionField.get(it) as? SQLiteConnection
        }

        val pointer = connection?.let {
            val mConnectionPtr = it.javaClass.getDeclaredField("mConnectionPtr")

            mConnectionPtr.isAccessible = true

            mConnectionPtr.get(it) as? Long
        }

        return pointer ?: 0L
    }

    fun releaseSQLiteConnection() {
        releaseSQLiteConnection(writerDb)
    }

    fun releaseSQLiteReadConnection() {
        releaseSQLiteConnection(readerDb)
    }

    private fun releaseSQLiteConnection(db: SQLiteDatabase) {
        val getThreadSession: Method = db.javaClass.getDeclaredMethod("getThreadSession")

        getThreadSession.isAccessible = true

        val session = getThreadSession.invoke(db) as? SQLiteSession

        session?.let {
            val releaseConnectionMethod = it.javaClass.getDeclaredMethod(
                "releaseConnection"
            )

            releaseConnectionMethod.isAccessible = true

            releaseConnectionMethod.invoke(session)
        }
    }

    fun unsafeExecuteStatements(statements: SQL) =
        transaction {
            // NOTE: This must NEVER be allowed to take user input - split by `;` is not grammer-aware
            // and so is unsafe. Only works with Watermelon-generated strings known to be safe
            // Replace ";END;" with a safe token so it survives splitting
            val safeStatements = statements
                .replace(";end;", "__END__;") // Protect END block
                .split(";")                  // Split safely (but doesn't kill END)
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .map { it.replace("__END__", ";end") } // Restore END

            safeStatements.forEach { execute(it) }
        }

    fun execute(query: SQL, args: QueryArgs = emptyArray()) =
        writerDb.execSQL(query, args)

    fun delete(query: SQL, args: QueryArgs) = writerDb.execSQL(query, args)

    fun rawQuery(query: SQL, args: RawQueryArgs = emptyArray()): Cursor = readDatabase(query).rawQuery(query, args)
    fun rawQueryOnWriter(query: SQL, args: RawQueryArgs = emptyArray()): Cursor = writerDb.rawQuery(query, args)

    fun count(query: SQL, args: RawQueryArgs = emptyArray()): Int =
        rawQuery(query, args).use {
            it.moveToFirst()
            return it.getInt(it.getColumnIndex("count"))
        }

    fun getFromLocalStorage(key: String): String? =
        rawQuery(Queries.select_local_storage, arrayOf(key)).use {
            it.moveToFirst()
            return if (it.count > 0) {
                it.getString(0)
            } else {
                null
            }
        }

    fun insertToLocalStorage(key: String, value: String) =
        execute(Queries.insert_local_storage, arrayOf(key, value))

    fun deleteFromLocalStorage(key: String) =
        execute(Queries.delete_local_storage, arrayOf(key))

//    fun unsafeResetDatabase() = context.deleteDatabase("$name.db")

    fun unsafeDestroyEverything() =
        transaction {
            getAllTables().forEach { execute(Queries.dropTable(it)) }
            execute("pragma writable_schema=1")
            execute("delete from sqlite_master where type in ('table', 'index', 'trigger')")
            execute("pragma user_version=0")
            execute("pragma writable_schema=0")
        }

    private fun getAllTables(): ArrayList<String> {
        val allTables: ArrayList<String> = arrayListOf()
        rawQuery(Queries.select_tables).use {
            it.moveToFirst()
            val index = it.getColumnIndex("name")
            if (index > -1) {
                do {
                    allTables.add(it.getString(index))
                } while (it.moveToNext())
            }
        }
        return allTables
    }

    fun transaction(function: () -> Unit) {
        // MOBILE-6492 (Tier 2): serialize against the native sync-apply path, which
        // writes to this same file via acquireSqliteConnection() below, bypassing this
        // method's own beginTransaction()/endTransaction() bookkeeping entirely.
        // Mirrors iOS's Database.swift#inTransaction (writerTransactionSemaphore.wait()
        // before beginTransaction(), signal() in the equivalent of `defer`).
        //
        // Bounded, unlike iOS's unconditional wait(): on timeout we proceed WITHOUT the
        // permit and let `PRAGMA busy_timeout` (Tier 1) arbitrate at the SQLite layer.
        // That deliberately degrades to the pre-Tier-2 behaviour — a possible
        // SQLiteDatabaseLockedException — rather than stalling this writer forever
        // behind a holder that may never release. Serialization is an optimization
        // here; never hanging is a correctness requirement.
        val acquired = acquireWriterTransactionSemaphore("js-action")
        if (!acquired) {
            Log.w(
                "watermelondb",
                "writer semaphore timed out after ${writerSemaphoreTimeoutMs}ms " +
                    "(holder=${currentWriterHolder() ?: "unknown"}); proceeding " +
                    "unserialized — busy_timeout will arbitrate"
            )
        }
        try {
            writerDb.beginTransaction()
            incrementTransactionDepth()

            try {
                function()
                writerDb.setTransactionSuccessful()
            } catch (e: SQLiteFullException) {
                e.printStackTrace()
                Log.e("watermelondb", "found this error ${e.localizedMessage}")
                throw e
            } finally {
                try {
                    writerDb.endTransaction()
                } catch (e: Exception) {
                    Log.e("watermelondb", "eee ${e.localizedMessage}")
                }
                decrementTransactionDepth()
            }
        } finally {
            // Only unwind a frame we actually entered. Releasing after a timed-out
            // acquire would inflate the permit count; and release() itself decides
            // whether this is the outermost frame, so the holder name survives for the
            // duration of a nested transaction.
            if (acquired) {
                releaseWriterTransactionSemaphore()
            }
        }
    }

    fun close() {
        writerDb.close()
        if (readerDb != writerDb) {
            readerDb.close()
        }
    }

    fun setUpdateHook(updateHook: SQLiteUpdateHook?) = writerDb.setUpdateHook(updateHook)

    private fun resolveDatabasePath(): String {
        // TODO: This SUCKS. Seems like Android doesn't like sqlite `?mode=memory&cache=shared` mode. To avoid random breakages, save the file to /tmp, but this is slow.
        // NOTE: This is because Android system SQLite is not compiled with SQLITE_USE_URI=1
        // issue `PRAGMA cache=shared` query after connection when needed
        return if (name == ":memory:" || name.contains("mode=memory")) {
            context.cacheDir.delete()
            File(context.cacheDir, name).path
        } else {
            // On some systems there is some kind of lock on `/databases` folder ¯\_(ツ)_/¯
            context.getDatabasePath("$name.db").path.replace("/databases", "")
        }
    }

    private fun readDatabase(query: SQL): SQLiteDatabase {
        if ((transactionDepth.get() ?: 0) > 0) {
            return writerDb
        }
        if (!isReadOnlyQuery(query)) {
            return writerDb
        }
        // Temp tables are per-connection and only exist on the writer.
        // Route queries referencing temp tables to the writer connection.
        if (referencesTemporaryTable(query)) {
            return writerDb
        }
        return readerDb
    }

    private fun isReadOnlyQuery(query: SQL): Boolean {
        val trimmed = query.trimStart().lowercase()
        return trimmed.startsWith("select") || trimmed.startsWith("with") || trimmed.startsWith("explain")
    }

    private fun referencesTemporaryTable(query: SQL): Boolean {
        val lower = query.lowercase()
        return lower.contains("temp.") || lower.contains("sqlite_temp_master")
    }

    private fun incrementTransactionDepth() {
        val next = (transactionDepth.get() ?: 0) + 1
        transactionDepth.set(next)
    }

    private fun decrementTransactionDepth() {
        val next = (transactionDepth.get() ?: 0) - 1
        if (next <= 0) {
            transactionDepth.remove()
        } else {
            transactionDepth.set(next)
        }
    }

    private fun isInMemoryPath(path: String): Boolean {
        return path == ":memory:" || path.contains("mode=memory")
    }

    internal fun _test_isReadOnlyQuery(query: SQL): Boolean {
        return isReadOnlyQuery(query)
    }

    internal fun _test_referencesTemporaryTable(query: SQL): Boolean {
        return referencesTemporaryTable(query)
    }

    internal fun _test_readDatabaseIdentity(query: SQL): String {
        return if (readDatabase(query) === readerDb) "reader" else "writer"
    }

    internal fun _test_readerQueryOnlyValue(): Int {
        val cursor = readerDb.rawQuery("pragma query_only", emptyArray())
        cursor.use {
            it.moveToFirst()
            return it.getInt(0)
        }
    }
}
