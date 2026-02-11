package com.nozbe.watermelondb.sync

import android.content.Context
import android.util.Log
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume

/**
 * WorkManager CoroutineWorker that performs a pull-only background sync
 * using the native C++ SyncEngine via BackgroundSyncBridge.
 */
class BackgroundSyncWorker(
    context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params) {

    companion object {
        private const val TAG = "WatermelonDB"
    }

    override suspend fun doWork(): Result {
        Log.i(TAG, "BackgroundSyncWorker: starting background sync")

        if (isStopped) {
            Log.i(TAG, "BackgroundSyncWorker: already stopped before starting")
            return Result.success()
        }

        return try {
            val (success, errorMessage) = suspendCancellableCoroutine { continuation ->
                BackgroundSyncBridge.performSync { syncSuccess, error ->
                    if (continuation.isActive) {
                        continuation.resume(Pair(syncSuccess, error))
                    }
                }
            }

            if (success) {
                Log.i(TAG, "BackgroundSyncWorker: sync completed successfully")
                Result.success()
            } else {
                Log.w(TAG, "BackgroundSyncWorker: sync failed: $errorMessage")
                Result.retry()
            }
        } catch (e: Exception) {
            Log.e(TAG, "BackgroundSyncWorker: exception during sync", e)
            Result.retry()
        }
    }
}
