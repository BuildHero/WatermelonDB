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

class Database(private val name: String, private val context: Context) {

    private val databasePath: String = resolveDatabasePath()
    private val transactionDepth = ThreadLocal<Int>()

    private val writerDb: SQLiteDatabase by lazy {
        SQLiteDatabase.openOrCreateDatabase(databasePath, null).also {
            runPragma(it, "PRAGMA journal_mode=WAL")
        }
    }

    private val readerDb: SQLiteDatabase by lazy {
        // Ensure writer is opened and WAL is enabled before opening the reader.
        writerDb
        if (isInMemoryPath(databasePath)) {
            writerDb
        } else {
            SQLiteDatabase.openDatabase(databasePath, null, SQLiteDatabase.OPEN_READONLY).also {
                try {
                    runPragma(it, "PRAGMA query_only=1")
                } catch (_: Exception) {
                    // Best effort; some builds may not allow setting pragmas on read-only connections.
                }
            }
        }
    }

    private fun runPragma(db: SQLiteDatabase, sql: String) {
        db.rawQuery(sql, null).use { /* pragma executed */ }
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
    }

    fun close() {
        writerDb.close()
        if (readerDb != writerDb) {
            readerDb.close()
        }
    }

    fun setUpdateHook(updateHook: SQLiteUpdateHook) = writerDb.setUpdateHook(updateHook)

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
        return if (isReadOnlyQuery(query)) readerDb else writerDb
    }

    private fun isReadOnlyQuery(query: SQL): Boolean {
        val trimmed = query.trimStart().lowercase()
        return trimmed.startsWith("select") || trimmed.startsWith("with") || trimmed.startsWith("explain")
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
