package com.nozbe.watermelondb

import android.database.SQLException
import android.os.Trace
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.WritableNativeArray
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReactContext
import com.facebook.react.bridge.WritableArray
import com.facebook.react.bridge.WritableMap
import com.facebook.react.modules.core.DeviceEventManagerModule
import com.nozbe.watermelondb.jsi.JSIAndroidBridgeInstaller
import io.requery.android.database.sqlite.SQLiteUpdateHook
import java.util.Timer
import java.util.TimerTask
import kotlin.collections.ArrayList

class DatabaseBridge(private val reactContext: ReactApplicationContext) :
    ReactContextBaseJavaModule(reactContext) {

    companion object {
        // Static singleton instance for Turbo Module access
        @Volatile
        private var INSTANCE: DatabaseBridge? = null
        
        fun getInstance(): DatabaseBridge? = INSTANCE
        
        private fun setInstance(instance: DatabaseBridge) {
            synchronized(this) {
                if (INSTANCE != null) {
                    // Log warning if we're replacing an existing instance
                    android.util.Log.w("WatermelonDB", "DatabaseBridge singleton being replaced. This may indicate multiple DatabaseBridge instances.")
                }
                INSTANCE = instance
            }
        }
    }

    init {
        // Store this instance as the singleton when created
        setInstance(this)
    }

    private val connections: MutableMap<ConnectionTag, Connection> = mutableMapOf()

    override fun getName(): String = "DatabaseBridge"

    sealed class Connection {
        class Connected(val driver: DatabaseDriver) : Connection()
        class Waiting(val queueInWaiting: ArrayList<(() -> Unit)>) : Connection()

        val queue: ArrayList<(() -> Unit)>
            get() = when (this) {
                is Connected -> arrayListOf()
                is Waiting -> this.queueInWaiting
            }
    }

    private val affectedTables = mutableSetOf<String>()
    private var batchTimer: Timer? = null
    private val batchInterval = 100L // milliseconds

    private val sqliteUpdateHook = SQLiteUpdateHook { _, _, tableName, _ ->
        bufferTableName(tableName)
        resetBatchTimer()
    }

    private fun bufferTableName(tableName: String) {
        // Add the table name to the set of affected tables (duplicates will be ignored)
        affectedTables.add(tableName)
    }

    private fun resetBatchTimer() {
        batchTimer?.cancel() // Cancel the existing timer if it's running

        batchTimer = Timer().apply {
            schedule(object : TimerTask() {
                override fun run() {
                    emitAffectedTables()
                }
            }, batchInterval)
        }
    }

    private fun emitAffectedTables() {
        if (affectedTables.isNotEmpty()) {
            val params = Arguments.createArray().apply {
                affectedTables.forEach { pushString(it) }
            }

            sendEvent(reactContext, "SQLITE_UPDATE_HOOK", params)

            affectedTables.clear() // Clear the buffer after emitting
        }

        batchTimer?.cancel()
        batchTimer = null
    }

    private fun sendEvent(reactContext: ReactContext, eventName: String, params: WritableArray?) {
        reactContext
            .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
            .emit(eventName, params)
    }

    @ReactMethod
    fun initialize(
        tag: ConnectionTag,
        databaseName: String,
        schemaVersion: Int,
        promise: Promise
    ) {
        assert(connections[tag] == null) { "A driver with tag $tag already set up" }
        val promiseMap = Arguments.createMap()

        try {
            connections[tag] = Connection.Connected(
                driver = DatabaseDriver(
                    context = reactContext,
                    dbName = databaseName,
                    schemaVersion = schemaVersion
                )
            )
            promiseMap.putString("code", "ok")
            promise.resolve(promiseMap)
        } catch (e: DatabaseDriver.SchemaNeededError) {
            connections[tag] = Connection.Waiting(queueInWaiting = arrayListOf())
            promiseMap.putString("code", "schema_needed")
            promise.resolve(promiseMap)
        } catch (e: DatabaseDriver.MigrationNeededError) {
            connections[tag] = Connection.Waiting(queueInWaiting = arrayListOf())
            promiseMap.putString("code", "migrations_needed")
            promiseMap.putInt("databaseVersion", e.databaseVersion)
            promise.resolve(promiseMap)
        } catch (e: Exception) {
            promise.reject(e)
        }
    }

    @ReactMethod(isBlockingSynchronousMethod = true)
    fun initializeJSIBridge() {
        val jsRuntime = reactContext.catalystInstance.javaScriptContextHolder.get()

        JSIAndroidBridgeInstaller.install(
            jsRuntime,
            this
        )
    }

    @ReactMethod
    fun setUpWithSchema(
        tag: ConnectionTag,
        databaseName: String,
        schema: SQL,
        schemaVersion: SchemaVersion,
        promise: Promise
    ) = connectDriver(
        connectionTag = tag,
        driver = DatabaseDriver(
            context = reactContext,
            dbName = databaseName,
            schema = Schema(
                version = schemaVersion,
                sql = schema
            )
        ),
        promise = promise
    )

    @ReactMethod
    fun setUpWithMigrations(
        tag: ConnectionTag,
        databaseName: String,
        migrations: SQL,
        fromVersion: SchemaVersion,
        toVersion: SchemaVersion,
        promise: Promise
    ) {
        try {
            connectDriver(
                connectionTag = tag,
                driver = DatabaseDriver(
                    context = reactContext,
                    dbName = databaseName,
                    migrations = MigrationSet(
                        from = fromVersion,
                        to = toVersion,
                        sql = migrations
                    )
                ),
                promise = promise
            )
        } catch (e: Exception) {
            disconnectDriver(tag)
            promise.reject(e)
        }
    }

    @ReactMethod
    fun find(tag: ConnectionTag, table: TableName, id: RecordID, promise: Promise) =
        withDriver(tag, promise) { it.find(table, id) }

    @ReactMethod
    fun query(tag: ConnectionTag, table: TableName, query: SQL, promise: Promise) =
        withDriver(tag, promise) { it.cachedQuery(table, query) }

    fun querySynchronous(tag: ConnectionTag, table: TableName, query: SQL): WritableArray {
        return withDriverSynchronous(tag) { it.cachedQuery(table, query) } as WritableArray
    }

    @ReactMethod
    fun execSqlQuery(tag: ConnectionTag, query: SQL, params: ReadableArray = WritableNativeArray(), promise: Promise) =
        withDriver(tag, promise) { it.execSqlQuery(query, params) }

    fun execSqlQuerySynchronous(tag: ConnectionTag, query: SQL, params: ReadableArray = WritableNativeArray()): WritableArray {
        return withDriverSynchronous(tag) { it.execSqlQuery(query, params) } as WritableArray
    }

    @ReactMethod
    fun count(tag: ConnectionTag, query: SQL, promise: Promise) =
        withDriver(tag, promise) { it.count(query) }

    @ReactMethod
    fun copyTables(tag: ConnectionTag, tables: ReadableArray, srcDB: String, promise: Promise) =
        withDriver(tag, promise) { it.copyTables(tables, srcDB) }

    @ReactMethod
    fun batch(tag: ConnectionTag, operations: ReadableArray, promise: Promise) =
        withDriver(tag, promise) { it.batch(operations) }

    @ReactMethod
    fun getDeletedRecords(tag: ConnectionTag, table: TableName, promise: Promise) =
        withDriver(tag, promise) { it.getDeletedRecords(table) }

    @ReactMethod
    fun destroyDeletedRecords(
        tag: ConnectionTag,
        table: TableName,
        records: ReadableArray,
        promise: Promise
    ) = withDriver(tag, promise) { it.destroyDeletedRecords(table, records.toArrayList().toArray()) }

    @ReactMethod
    fun unsafeResetDatabase(
        tag: ConnectionTag,
        schema: SQL,
        schemaVersion: SchemaVersion,
        promise: Promise
    ) = withDriver(tag, promise) { it.unsafeResetDatabase(Schema(schemaVersion, schema)) }

    @ReactMethod
    fun getLocal(tag: ConnectionTag, key: String, promise: Promise) =
        withDriver(tag, promise) { it.getLocal(key) }

    @ReactMethod
    fun setLocal(tag: ConnectionTag, key: String, value: String, promise: Promise) =
        withDriver(tag, promise) { it.setLocal(key, value) }

    @ReactMethod
    fun removeLocal(tag: ConnectionTag, key: String, promise: Promise) =
        withDriver(tag, promise) { it.removeLocal(key) }

    @ReactMethod
    fun enableNativeCDC(tag: ConnectionTag, promise: Promise) =
        withDriver(tag, promise) { it.setUpdateHook(sqliteUpdateHook) }

    fun getSQLiteConnection(tag: ConnectionTag): Long {
        val driver = getDriver(tag)

        return driver.getDatabase().acquireSqliteConnection()
    }

    fun releaseSQLiteConnection(tag: ConnectionTag) {
        val driver = getDriver(tag)

        driver.getDatabase().releaseSQLiteConnection()
    }

    fun isCached(tag: ConnectionTag, table: TableName, id: RecordID): Boolean {
        val driver = getDriver(tag)

        return driver.isCached(table, id)
    }

    fun markAsCached(tag: ConnectionTag, table: TableName, id: RecordID) {
        val driver = getDriver(tag)

        return driver.markAsCached(table, id)
    }

    private fun getDriver(tag: ConnectionTag): DatabaseDriver {
        val connection = connections[tag] ?: throw Exception("No driver with tag $tag available")

        return when (connection) {
            is Connection.Connected -> connection.driver
            is Connection.Waiting -> throw Exception("Driver with tag $tag is not ready")
        }
    }

    @Throws(Exception::class)
    private fun withDriverSynchronous(
        tag: ConnectionTag,
        function: (DatabaseDriver) -> Any?
    ): Any? {
        val connection = connections[tag] ?: throw Exception("No driver with tag $tag available")

        return when (connection) {
            is Connection.Connected -> function(connection.driver)
            is Connection.Waiting -> throw Exception("Driver with tag $tag is not ready")
        }
    }

    @Throws(Exception::class)
    private fun withDriver(
        tag: ConnectionTag,
        promise: Promise,
        function: (DatabaseDriver) -> Any?
    ) {
        val functionName = function.javaClass.enclosingMethod?.name
        try {
            Trace.beginSection("DatabaseBridge.$functionName")
            when (val connection =
                connections[tag] ?: promise.reject(
                    Exception("No driver with tag $tag available"))) {
                is Connection.Connected -> {
                    val result = function(connection.driver)
                    promise.resolve(if (result === Unit) {
                        true
                    } else {
                        result
                    })
                }
                is Connection.Waiting -> {
                    // try again when driver is ready
                    connection.queue.add { withDriver(tag, promise, function) }
                    connections[tag] = Connection.Waiting(connection.queue)
                }
            }
        } catch (e: SQLException) {
            promise.reject(functionName ?: "UnknownFunction", e)
        } finally {
            Trace.endSection()
        }
    }

    private fun connectDriver(
        connectionTag: ConnectionTag,
        driver: DatabaseDriver,
        promise: Promise
    ) {
        val queue = connections[connectionTag]?.queue ?: arrayListOf()
        connections[connectionTag] = Connection.Connected(driver)

        for (operation in queue) {
            operation()
        }
        promise.resolve(true)
    }

    private fun disconnectDriver(connectionTag: ConnectionTag) {
        val queue = connections[connectionTag]?.queue ?: arrayListOf()
        connections.remove(connectionTag)

        for (operation in queue) {
            operation()
        }
    }
}
