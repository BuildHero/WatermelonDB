package com.nozbe.watermelondb.sync

import android.content.Context
import android.util.Log
import androidx.work.*
import org.json.JSONObject
import java.util.concurrent.TimeUnit

/**
 * Schedules background sync tasks using WorkManager.
 *
 * Provides:
 * - Periodic sync (~15 min default) via PeriodicWorkRequest
 * - Mutation-driven one-shot sync via OneTimeWorkRequest
 * - Configuration via JSON from JS bridge
 */
object BackgroundSyncScheduler {

    private const val TAG = "WatermelonDB"
    private const val PERIODIC_WORK_NAME = "watermelondb_periodic_sync"
    private const val ONESHOT_WORK_NAME = "watermelondb_mutation_sync"

    @Volatile
    private var intervalMinutes: Long = 15
    @Volatile
    private var requiresNetwork: Boolean = true
    @Volatile
    private var appContext: Context? = null
    @Volatile
    private var mutationQueueTable: String? = null

    /**
     * Configure background sync from JSON config string.
     * Called from JNI (JSIAndroidBridgeModule::configureBackgroundSync).
     */
    // WorkManager minimum periodic interval is 15 minutes
    private const val MIN_INTERVAL_MINUTES = 15L

    @JvmStatic
    fun configure(configJson: String) {
        try {
            val json = JSONObject(configJson)
            val requested = json.optLong("intervalMinutes", 15)
            intervalMinutes = maxOf(requested, MIN_INTERVAL_MINUTES)
            if (requested < MIN_INTERVAL_MINUTES) {
                Log.w(TAG, "BackgroundSync: requested interval ${requested}min clamped to minimum ${MIN_INTERVAL_MINUTES}min")
            }
            requiresNetwork = json.optBoolean("requiresNetwork", true)
            mutationQueueTable = json.optString("mutationQueueTable", null)
            Log.i(TAG, "BackgroundSync configured: interval=${intervalMinutes}min, network=$requiresNetwork, mutationTable=$mutationQueueTable")
        } catch (e: Exception) {
            Log.e(TAG, "BackgroundSync configure failed: ${e.message}")
        }
    }

    /**
     * Get the configured mutation queue table name.
     */
    @JvmStatic
    fun getMutationQueueTable(): String? = mutationQueueTable

    /**
     * Initialize with application context. Must be called before scheduling.
     * Typically called from DatabaseBridge or Application.onCreate.
     */
    @JvmStatic
    fun initContext(context: Context) {
        appContext = context.applicationContext
    }

    /**
     * Schedule periodic background sync.
     * Called from JNI (JSIAndroidBridgeModule::enableBackgroundSync).
     */
    @JvmStatic
    fun enablePeriodicSync() {
        val context = appContext
        if (context == null) {
            Log.w(TAG, "BackgroundSync: no app context, cannot schedule periodic sync")
            return
        }

        val constraints = Constraints.Builder()
            .setRequiredNetworkType(
                if (requiresNetwork) NetworkType.CONNECTED else NetworkType.NOT_REQUIRED
            )
            .build()

        val request = PeriodicWorkRequestBuilder<BackgroundSyncWorker>(
            intervalMinutes, TimeUnit.MINUTES
        )
            .setConstraints(constraints)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            PERIODIC_WORK_NAME,
            ExistingPeriodicWorkPolicy.UPDATE,
            request
        )
        Log.i(TAG, "BackgroundSync: periodic sync scheduled (interval: ${intervalMinutes} min)")
    }

    /**
     * Schedule an immediate one-shot background sync (mutation-driven).
     * Called from DatabaseBridge when mutation queue table is modified in background.
     */
    @JvmStatic
    fun scheduleMutationDrivenSync() {
        val context = appContext ?: return

        val constraints = Constraints.Builder()
            .setRequiredNetworkType(
                if (requiresNetwork) NetworkType.CONNECTED else NetworkType.NOT_REQUIRED
            )
            .build()

        val request = OneTimeWorkRequestBuilder<BackgroundSyncWorker>()
            .setConstraints(constraints)
            .build()

        WorkManager.getInstance(context).enqueueUniqueWork(
            ONESHOT_WORK_NAME,
            ExistingWorkPolicy.REPLACE,
            request
        )
        Log.i(TAG, "BackgroundSync: mutation-driven sync scheduled")
    }

    /**
     * Cancel all scheduled background sync tasks.
     * Called from JNI (JSIAndroidBridgeModule::disableBackgroundSync).
     */
    @JvmStatic
    fun cancelAll() {
        val context = appContext
        if (context == null) {
            Log.w(TAG, "BackgroundSync: no app context, cannot cancel")
            return
        }

        WorkManager.getInstance(context).cancelUniqueWork(PERIODIC_WORK_NAME)
        WorkManager.getInstance(context).cancelUniqueWork(ONESHOT_WORK_NAME)
        Log.i(TAG, "BackgroundSync: all tasks cancelled")
    }
}
