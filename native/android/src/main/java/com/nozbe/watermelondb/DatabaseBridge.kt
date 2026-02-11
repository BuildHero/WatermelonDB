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
import com.nozbe.watermelondb.sync.BackgroundSyncBridge
import com.nozbe.watermelondb.sync.BackgroundSyncScheduler
import io.requery.android.database.sqlite.SQLiteUpdateHook
import java.util.Timer
import java.util.TimerTask
import kotlin.collections.ArrayList

class DatabaseBridge(private val reactContext: ReactApplicationContext) :
    ReactContextBaseJavaModule(reactContext) {

    companion object {
        @Volatile
        private var INSTANCE: DatabaseBridge? = null
        
        sealed class Connection {
            class Connected(val driver: DatabaseDriver) : Connection()
            class Waiting(val queueInWaiting: ArrayList<(() -> Unit)>) : Connection()

            val queue: ArrayList<(() -> Unit)>
                get() = when (this) {
                    is Connected -> arrayListOf()
                    is Waiting -> this.queueInWaiting
                }
        }
        
        private val connections: MutableMap<ConnectionTag, Connection> = mutableMapOf()
        
        data class ConnectionMetadata(
            val databaseName: String,
            val schemaVersion: Int
        )
        
        private val connectionMetadata: MutableMap<ConnectionTag, ConnectionMetadata> = mutableMapOf()
        
        fun getInstance(): DatabaseBridge? = INSTANCE
        
        private fun setInstance(instance: DatabaseBridge) {
            synchronized(this) {
                if (INSTANCE != null) {
                    android.util.Log.i("WatermelonDB", "DatabaseBridge instance being replaced. Preserving ${connections.size} existing connections.")
                }
                INSTANCE = instance
            }
        }
    }

    init {
        setInstance(this)
        android.util.Log.i("WatermelonDB", "DatabaseBridge initialized with ${connections.size} existing connections")
        // Initialize background sync utilities with application context
        try {
            val appContext = reactContext.applicationContext
            BackgroundSyncScheduler.initContext(appContext)
        } catch (e: Exception) {
            android.util.Log.w("WatermelonDB", "Failed to init background sync context: ${e.message}")
        }
    }

    override fun getName(): String = "DatabaseBridge"

    private val affectedTables = mutableSetOf<String>()
    private var batchTimer: Timer? = null
    private var batchTimerTask: TimerTask? = null
    private val batchInterval = 100L // milliseconds

    private var _mutationQueueTable: String? = null
    private var _backgroundSyncEnabled = false

    private val sqliteUpdateHook = SQLiteUpdateHook { _, _, tableName, _ ->
        bufferTableName(tableName)
        resetBatchTimer()

        // Schedule background sync when mutation queue table is modified while app is backgrounded
        if (_backgroundSyncEnabled && tableName == _mutationQueueTable) {
            try {
                if (!BackgroundSyncBridge.isAppInForeground()) {
                    BackgroundSyncScheduler.scheduleMutationDrivenSync()
                }
            } catch (e: Exception) {
                android.util.Log.w("WatermelonDB", "Failed to schedule mutation-driven sync: ${e.message}")
            }
        }
    }

    override fun invalidate() {
        super.invalidate()
        // Clean up resources when module is destroyed
        cleanupCDC()
    }

    private fun cleanupCDC() {
        synchronized(this) {
            // Safely cancel timer task
            try {
                batchTimerTask?.cancel()
                batchTimerTask = null
            } catch (e: Exception) {
                android.util.Log.w("WatermelonDB", "Error canceling timer task: ${e.message}")
            }

            // Safely cleanup timer
            try {
                batchTimer?.cancel()
                batchTimer?.purge()
                batchTimer = null
            } catch (e: Exception) {
                android.util.Log.w("WatermelonDB", "Error cleaning up timer: ${e.message}")
            }

            affectedTables.clear()
        }
    }

    private fun bufferTableName(tableName: String) {
        // Add the table name to the set of affected tables (duplicates will be ignored)
        // Thread-safe access to shared mutable state
        synchronized(this) {
            affectedTables.add(tableName)
        }
    }

    private fun resetBatchTimer() {
        // Thread-safe timer management - reuse single Timer to avoid thread leaks
        synchronized(this) {
            // Cancel existing task
            batchTimerTask?.cancel()

            // Create Timer if it doesn't exist (lazy initialization)
            if (batchTimer == null) {
                batchTimer = Timer("WatermelonDB-CDC-Timer", true) // daemon thread
            }

            // Schedule new task
            batchTimerTask = object : TimerTask() {
                override fun run() {
                    emitAffectedTables()
                }
            }
            batchTimer?.schedule(batchTimerTask, batchInterval)
        }
    }

    private fun emitAffectedTables() {
        // Copy state under lock to avoid holding lock during event emission
        val tables: List<String>
        synchronized(this) {
            if (affectedTables.isEmpty()) {
                return
            }
            tables = affectedTables.toList()
            affectedTables.clear()
            batchTimerTask = null
        }

        val params = Arguments.createArray().apply {
            tables.forEach { pushString(it) }
        }

        sendEvent(reactContext, "SQLITE_UPDATE_HOOK", params)
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
        val existingConnection = connections[tag]
        if (existingConnection != null) {
            android.util.Log.i("WatermelonDB", "Connection with tag $tag already exists. Reusing existing connection.")
            val promiseMap = Arguments.createMap()
            promiseMap.putString("code", "ok")
            promise.resolve(promiseMap)
            return
        }
        
        val promiseMap = Arguments.createMap()

        try {
            connectionMetadata[tag] = ConnectionMetadata(databaseName, schemaVersion)
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

    @ReactMethod
    fun setUpWithSchema(
        tag: ConnectionTag,
        databaseName: String,
        schema: SQL,
        schemaVersion: SchemaVersion,
        promise: Promise
    ) {
        connectionMetadata[tag] = ConnectionMetadata(databaseName, schemaVersion)
        connectDriver(
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
    }

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
            connectionMetadata[tag] = ConnectionMetadata(databaseName, toVersion)
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

    fun configureBackgroundSync(mutationTable: String?) {
        _mutationQueueTable = mutationTable
        _backgroundSyncEnabled = mutationTable != null
    }

    @ReactMethod
    fun enableNativeCDC(tag: ConnectionTag, promise: Promise) =
        withDriver(tag, promise) { it.setUpdateHook(sqliteUpdateHook) }

    @ReactMethod
    fun disableNativeCDC(tag: ConnectionTag, promise: Promise) {
        // Clean up the batch timer and affected tables FIRST to prevent race conditions
        // This must happen before disabling the hook to ensure no events fire during cleanup
        cleanupCDC()

        withDriver(tag, promise) { it.disableUpdateHook() }
    }

    @ReactMethod
    fun setCDCEnabled(tag: ConnectionTag, enabled: Boolean, promise: Promise) =
        withDriver(tag, promise) { it.setCDCEnabled(enabled) }

    fun getSQLiteConnection(tag: ConnectionTag): Long {
        try {
            val driver = getDriver(tag)
            return driver.getDatabase().acquireSqliteConnection()
        } catch (e: Exception) {
            android.util.Log.e("WatermelonDB", "getSQLiteConnection failed for tag $tag", e)
            throw Exception("Failed to get SQLite connection for tag $tag: ${e.message}", e)
        }
    }

    fun releaseSQLiteConnection(tag: ConnectionTag) {
        try {
            val driver = getDriver(tag)
            driver.getDatabase().releaseSQLiteConnection()
        } catch (e: Exception) {
            android.util.Log.e("WatermelonDB", "releaseSQLiteConnection failed for tag $tag", e)
        }
    }

    fun getSQLiteReadConnection(tag: ConnectionTag): Long {
        try {
            val driver = getDriver(tag)
            return driver.getDatabase().acquireSqliteReadConnection()
        } catch (e: Exception) {
            android.util.Log.e("WatermelonDB", "getSQLiteReadConnection failed for tag $tag", e)
            throw Exception("Failed to get SQLite read connection for tag $tag: ${e.message}", e)
        }
    }

    fun releaseSQLiteReadConnection(tag: ConnectionTag) {
        try {
            val driver = getDriver(tag)
            driver.getDatabase().releaseSQLiteReadConnection()
        } catch (e: Exception) {
            android.util.Log.e("WatermelonDB", "releaseSQLiteReadConnection failed for tag $tag", e)
        }
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
        val connection = connections[tag]
        
        if (connection == null) {
            val metadata = connectionMetadata[tag]
            if (metadata != null) {
                android.util.Log.i("WatermelonDB", "Connection lost for tag $tag. Attempting auto-recovery with database: ${metadata.databaseName}")
                try {
                    val driver = DatabaseDriver(
                        context = reactContext,
                        dbName = metadata.databaseName
                    )
                    connections[tag] = Connection.Connected(driver)
                    android.util.Log.i("WatermelonDB", "Successfully recovered connection for tag $tag")
                    return driver
                } catch (e: Exception) {
                    android.util.Log.e("WatermelonDB", "Failed to recover connection for tag $tag", e)
                    connectionMetadata.remove(tag)
                    throw Exception("Failed to recover connection for tag $tag: ${e.message}. Connection may not be initialized or was disconnected.", e)
                }
            }
            
            android.util.Log.w("WatermelonDB", "No driver with tag $tag available. Connection may not be initialized or was disconnected. Available tags: ${connections.keys.joinToString()}")
            throw Exception("No driver with tag $tag available. Connection may not be initialized or was disconnected.")
        }

        return when (connection) {
            is Connection.Connected -> connection.driver
            is Connection.Waiting -> {
                android.util.Log.w("WatermelonDB", "Driver with tag $tag is not ready - still initializing")
                throw Exception("Driver with tag $tag is not ready - still initializing")
            }
        }
    }

    @Throws(Exception::class)
    private fun withDriverSynchronous(
        tag: ConnectionTag,
        function: (DatabaseDriver) -> Any?
    ): Any? {
        val connection = connections[tag]
        
        if (connection == null) {
            android.util.Log.w("WatermelonDB", "No driver with tag $tag available. Connection may not be initialized or was disconnected. Available tags: ${connections.keys.joinToString()}")
            throw Exception("No driver with tag $tag available. Connection may not be initialized or was disconnected.")
        }

        return when (connection) {
            is Connection.Connected -> function(connection.driver)
            is Connection.Waiting -> {
                android.util.Log.w("WatermelonDB", "Driver with tag $tag is not ready - still initializing")
                throw Exception("Driver with tag $tag is not ready - still initializing")
            }
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
            val connection = connections[tag]
            
            if (connection == null) {
                android.util.Log.w("WatermelonDB", "No driver with tag $tag available for $functionName. Connection may not be initialized or was disconnected. Available tags: ${connections.keys.joinToString()}")
                promise.reject(
                    "ConnectionNotFound",
                    "No driver with tag $tag available. Connection may not be initialized or was disconnected.",
                    null
                )
                return
            }
            
            when (connection) {
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
        connectionMetadata.remove(connectionTag)

        for (operation in queue) {
            operation()
        }
    }
}
