package com.nozbe.watermelondb.sync

import android.os.Handler
import android.os.Looper
import android.util.Log
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.ProcessLifecycleOwner

/**
 * Bridge between Kotlin (WorkManager) and C++ (SyncEngine) for background sync.
 *
 * Stores a reference to the native SyncEngine pointer for JNI calls.
 */
object BackgroundSyncBridge {

    private const val TAG = "WatermelonDB"

    @Volatile
    private var syncEnginePtr: Long = 0

    @Volatile
    private var foregroundObserverRegistered = false

    /**
     * Configure the bridge with the native SyncEngine pointer.
     * Called from JNI after SyncEngine is created.
     */
    @JvmStatic
    fun configure(enginePtr: Long) {
        this.syncEnginePtr = enginePtr
        Log.i(TAG, "BackgroundSyncBridge configured: enginePtr=$enginePtr")
        registerForegroundObserver()
    }

    /**
     * Register a lifecycle observer that cancels any in-flight background sync
     * the moment the app returns to foreground — before the JS runtime processes events.
     */
    private fun registerForegroundObserver() {
        if (foregroundObserverRegistered) return
        foregroundObserverRegistered = true
        Handler(Looper.getMainLooper()).post {
            try {
                ProcessLifecycleOwner.get().lifecycle.addObserver(
                    LifecycleEventObserver { _, event ->
                        if (event == Lifecycle.Event.ON_START && syncEnginePtr != 0L) {
                            Log.i(TAG, "App entering foreground — cancelling background sync")
                            try {
                                nativeCancelBackgroundSync(syncEnginePtr)
                            } catch (e: Exception) {
                                Log.w(TAG, "Failed to cancel background sync on foreground", e)
                            }
                        }
                    }
                )
            } catch (e: Exception) {
                Log.w(TAG, "Failed to register foreground observer", e)
                foregroundObserverRegistered = false
            }
        }
    }

    /**
     * Check if the app is currently in the foreground.
     * Used by DatabaseBridge mutation queue hook to decide whether to schedule background sync.
     */
    @JvmStatic
    fun isAppInForeground(): Boolean {
        return try {
            ProcessLifecycleOwner.get().lifecycle.currentState.isAtLeast(Lifecycle.State.RESUMED)
        } catch (e: Exception) {
            // If lifecycle owner is not available, assume foreground to avoid unnecessary scheduling
            true
        }
    }

    /**
     * Perform a pull-only background sync.
     * Called from BackgroundSyncWorker.doWork().
     *
     * The SyncEngine uses its normal auth flow: if authToken_ is empty it calls
     * authTokenRequestCallback_ which reaches the JS auth provider (works when
     * JS runtime is still alive in background).
     */
    @JvmStatic
    fun performSync(completion: (Boolean, String?) -> Unit) {
        if (syncEnginePtr == 0L) {
            Log.w(TAG, "BackgroundSyncBridge: no sync engine configured")
            completion(false, "No sync engine configured")
            return
        }

        try {
            nativePerformBackgroundSync(syncEnginePtr, object : NativeSyncCallback {
                override fun onComplete(success: Boolean, errorMessage: String?) {
                    completion(success, errorMessage)
                }
            })
        } catch (e: Exception) {
            Log.e(TAG, "BackgroundSyncBridge: native sync failed", e)
            completion(false, e.message)
        }
    }

    /**
     * Callback interface for JNI sync completion.
     */
    interface NativeSyncCallback {
        fun onComplete(success: Boolean, errorMessage: String?)
    }

    /**
     * JNI method: performs pull-only sync using native SyncEngine.
     * Overrides push to no-op, calls startWithCompletion.
     */
    private external fun nativePerformBackgroundSync(
        enginePtr: Long,
        callback: NativeSyncCallback
    )

    init {
        try {
            System.loadLibrary("watermelon-jsi-android-bridge")
        } catch (e: UnsatisfiedLinkError) {
            Log.w(TAG, "BackgroundSyncBridge: could not load native library", e)
        }
    }
}
