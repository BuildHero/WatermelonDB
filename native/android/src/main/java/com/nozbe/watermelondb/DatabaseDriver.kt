package com.nozbe.watermelondb

import android.os.Trace
import android.content.Context
import android.database.Cursor
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.WritableNativeArray
import com.facebook.react.bridge.WritableArray
import io.requery.android.database.sqlite.SQLiteUpdateHook
import java.lang.Exception
import java.util.logging.Logger

class DatabaseDriver(context: Context, dbName: String) {
    sealed class Operation {
        class Execute(val table: TableName, val query: SQL, val args: QueryArgs) : Operation()
        class Create(val table: TableName, val id: RecordID, val query: SQL, val args: QueryArgs) :
                Operation()

        class MarkAsDeleted(val table: TableName, val id: RecordID) : Operation()
        class DestroyPermanently(val table: TableName, val id: RecordID) : Operation()
        // class SetLocal(val key: String, val value: String) : Operation()
        // class RemoveLocal(val key: String) : Operation()
    }

    class SchemaNeededError : Exception()
    data class MigrationNeededError(val databaseVersion: SchemaVersion) : Exception()

    constructor(context: Context, dbName: String, schemaVersion: SchemaVersion) :
            this(context, dbName) {
        when (val compatibility = isCompatible(schemaVersion)) {
            is SchemaCompatibility.NeedsSetup -> throw SchemaNeededError()
            is SchemaCompatibility.NeedsMigration ->
                throw MigrationNeededError(compatibility.fromVersion)
            else -> {}
        }
    }

    constructor(context: Context, dbName: String, schema: Schema) : this(context, dbName) {
        unsafeResetDatabase(schema)
    }

    constructor(context: Context, dbName: String, migrations: MigrationSet) :
            this(context, dbName) {
        migrate(migrations)
    }

    private val database: Database = Database(dbName, context)

    private val log: Logger? = if (BuildConfig.DEBUG) Logger.getLogger("DB_Driver") else null

    private val cachedRecords: MutableMap<TableName, MutableList<RecordID>> = mutableMapOf()

    fun getDatabase() = database

    fun find(table: TableName, id: RecordID): Any? {
        if (isCached(table, id)) {
            return id
        }
        database.rawQuery("select * from `$table` where id == ? limit 1", arrayOf(id)).use {
            if (it.count <= 0) {
                return null
            }
            val resultMap = Arguments.createMap()
            markAsCached(table, id)
            it.moveToFirst()
            resultMap.mapCursor(it)
            return resultMap
        }
    }

    fun cachedQuery(table: TableName, query: SQL): WritableArray {
        // log?.info("Cached Query: $query")
        val resultArray = Arguments.createArray()
        database.rawQuery(query).use {
            if (it.count > 0 && it.columnNames.contains("id")) {
                while (it.moveToNext()) {
                    val id = it.getString(it.getColumnIndex("id"))
                    if (isCached(table, id)) {
                        resultArray.pushString(id)
                    } else {
                        markAsCached(table, id)
                        resultArray.pushMapFromCursor(it)
                    }
                }
            }
        }
        return resultArray
    }

    fun execSqlQuery(query: SQL, params: ReadableArray = WritableNativeArray()): WritableArray {
        val resultArray = Arguments.createArray()
        val sqlParams = mutableListOf<String>()

        for (i in 0 until params.size()) {
            sqlParams.add(params.getString(i))
        }

        database.rawQuery(query, sqlParams.toTypedArray()).use {
            if (it.count > 0) {
                while (it.moveToNext()) {
                    resultArray.pushMapFromCursor(it)
                }
            }
        }

        return resultArray
    }

    private fun WritableArray.pushMapFromCursor(cursor: Cursor) {
        val cursorMap = Arguments.createMap()
        cursorMap.mapCursor(cursor)
        this.pushMap(cursorMap)
    }

    fun getDeletedRecords(table: TableName): WritableArray {
        val resultArray = Arguments.createArray()
        database.rawQuery(Queries.selectDeletedIdsFromTable(table)).use {
            it.moveToFirst()
            for (i in 0 until it.count) {
                resultArray.pushString(it.getString(0))
                it.moveToNext()
            }
        }
        return resultArray
    }

    fun destroyDeletedRecords(table: TableName, records: QueryArgs) =
            database.delete(Queries.multipleDeleteFromTable(table, records), records)

    fun count(query: SQL): Int = database.count(query)

    private fun execute(query: SQL, args: QueryArgs) {
        // log?.info("Executing: $query")
        database.execute(query, args)
    }

    fun getLocal(key: String): String? {
        // log?.info("Get Local: $key")
        return database.getFromLocalStorage(key)
    }

    fun setLocal(key: String, value: String) {
        // log?.info("Set Local: $key -> $value")
        database.insertToLocalStorage(key, value)
    }

    fun removeLocal(key: String) {
        log?.info("Remove local: $key")
        database.deleteFromLocalStorage(key)
    }

    private fun create(id: RecordID, query: SQL, args: QueryArgs) {
        // log?.info("Create id: $id query: $query")
        database.execute(query, args)
    }

    private fun getColumnNames(tableName: String, db: Database, schema: String = "main"): Set<String> {
        val columnNames = mutableSetOf<String>()
    
        // Execute the PRAGMA table_info command to get the table schema information
        db.rawQuery("PRAGMA $schema.table_info($tableName)").use { cursor ->
            if (cursor.count > 0 && cursor.columnNames.contains("name")) {
                while (cursor.moveToNext()) {
                    // Correctly fetch the column name using "name" as the column index
                    val columnName = cursor.getString(cursor.getColumnIndex("name"))
                    columnNames.add(columnName)
                }
            }
        }
        
        return columnNames
    }
    fun copyTables(tables: ReadableArray, srcDB: String) {
        // Attach the source database
        database.execute("ATTACH DATABASE '${srcDB}' as 'other'")

        // We need to make sure we do not copy the __watermelon_last_pulled_schema_version entry
        // from local_storage table to avoid migration issues
        database.execute("DELETE FROM local_storage WHERE key = '__watermelon_last_pulled_schema_version'")
    
        database.transaction {
            for (i in 0 until tables.size()) {
                val table = tables.getString(i)
            
                // Get the list of columns in the destination database
                val destColumns = getColumnNames(table, database)
            
                // Get the list of columns in the source database
                val srcColumns = getColumnNames(table, database, "other")
            
                // Find the intersection of the column names
                val commonColumns = destColumns.intersect(srcColumns)
            
                if (commonColumns.isNotEmpty()) {
                    // Escape the common column names for use in SQL query
                    val escapedColumns = commonColumns.joinToString(", ") { columnName ->
                        "\"$columnName\""
                    }
            
                    // Perform the data import using the escaped common columns
                    database.execute(
                        """
                        INSERT OR IGNORE INTO $table ($escapedColumns)
                        SELECT $escapedColumns FROM other.$table
                        """.trimIndent()
                    )
                }
            }
        }
    
        // Detach the source database
        database.execute("DETACH DATABASE 'other'")
    }
    
    fun batch(operations: ReadableArray) {
        // log?.info("Batch of ${operations.size()}")
        val newIds = arrayListOf<Pair<TableName, RecordID>>()
        val removedIds = arrayListOf<Pair<TableName, RecordID>>()

        Trace.beginSection("Batch")
        try {
            database.transaction {
                for (i in 0 until operations.size()) {
                    val operation = operations.getArray(i)
                    val type = operation?.getString(0)
                    when (type) {
                        "execute" -> {
                            val query = operation.getString(2) as SQL
                            val args = operation.getArray(3)!!.toArrayList().toArray()
                            execute(query, args)
                        }
                        "create" -> {
                            val table = operation.getString(1) as TableName
                            val id = operation.getString(2) as RecordID
                            val query = operation.getString(3) as SQL
                            val args = operation.getArray(4)!!.toArrayList().toArray()
                            create(id, query, args)
                            newIds.add(Pair(table, id))
                        }
                        "markAsDeleted" -> {
                            val table = operation.getString(1) as TableName
                            val id = operation.getString(2) as RecordID
                            database.execute(Queries.setStatusDeleted(table), arrayOf(id))
                            removedIds.add(Pair(table, id))
                        }
                        "destroyPermanently" -> {
                            val table = operation.getString(1) as TableName
                            val id = operation.getString(2) as RecordID
                            database.execute(Queries.destroyPermanently(table), arrayOf(id))
                            removedIds.add(Pair(table, id))
                        }
                        // "setLocal" -> {
                        //     val key = operation.getString(1)
                        //     val value = operation.getString(2)
                        //     preparedOperations.add(Operation.SetLocal(key, value))
                        // }
                        // "removeLocal" -> {
                        //     val key = operation.getString(1)
                        //     preparedOperations.add(Operation.RemoveLocal(key))
                        // }
                        else -> throw (Throwable("Bad operation name in batch"))
                    }
                }
            }
        } finally {
            Trace.endSection()
        }

        Trace.beginSection("updateCaches")
        newIds.forEach { markAsCached(table = it.first, id = it.second) }
        removedIds.forEach { removeFromCache(table = it.first, id = it.second) }
        Trace.endSection()
    }

    fun unsafeResetDatabase(schema: Schema) {
        log?.info("Unsafe Reset Database")
        database.unsafeDestroyEverything()
        cachedRecords.clear()
        setUpSchema(schema)
    }

    fun setUpdateHook(updateHook: SQLiteUpdateHook) = database.setUpdateHook(updateHook)

    fun close() = database.close()

    fun markAsCached(table: TableName, id: RecordID) {
        // log?.info("Mark as cached $id")
        val cache = cachedRecords[table] ?: mutableListOf()
        cache.add(id)
        cachedRecords[table] = cache
    }

    fun isCached(table: TableName, id: RecordID): Boolean =
            cachedRecords[table]?.contains(id) ?: false

    private fun removeFromCache(table: TableName, id: RecordID) = cachedRecords[table]?.remove(id)

    private fun setUpSchema(schema: Schema) {
        database.transaction {
            database.unsafeExecuteStatements(schema.sql + Queries.localStorageSchema)
            database.userVersion = schema.version
        }
    }

    private fun migrate(migrations: MigrationSet) {
        require(database.userVersion == migrations.from) {
            "Incompatible migration set applied. " +
                    "DB: ${database.userVersion}, migration: ${migrations.from}"
        }

        database.transaction {
            database.unsafeExecuteStatements(migrations.sql)
            database.userVersion = migrations.to
        }
    }

    sealed class SchemaCompatibility {
        object Compatible : SchemaCompatibility()
        object NeedsSetup : SchemaCompatibility()
        class NeedsMigration(val fromVersion: SchemaVersion) : SchemaCompatibility()
    }

    private fun isCompatible(schemaVersion: SchemaVersion): SchemaCompatibility =
            when (val databaseVersion = database.userVersion) {
                schemaVersion -> SchemaCompatibility.Compatible
                0 -> SchemaCompatibility.NeedsSetup
                in 1 until schemaVersion ->
                    SchemaCompatibility.NeedsMigration(fromVersion = databaseVersion)
                else -> {
                    log?.info("Database has newer version ($databaseVersion) than what the " +
                            "app supports ($schemaVersion). Will reset database.")
                    SchemaCompatibility.NeedsSetup
                }
            }
}
