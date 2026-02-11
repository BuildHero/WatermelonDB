import Foundation
import BackgroundTasks

/// Native background sync scheduling for WatermelonDB using iOS BGTaskScheduler.
///
/// Usage:
/// 1. Call `register(taskId:)` from AppDelegate `didFinishLaunchingWithOptions` BEFORE RN init.
/// 2. Add the taskId to Info.plist `BGTaskSchedulerPermittedIdentifiers`.
/// 3. Call `configure(intervalMinutes:requiresNetwork:)` from JS bridge.
/// 4. Call `schedulePeriodicSync()` to begin scheduling.
/// 5. Call `cancelAllScheduledTasks()` to stop.
@objc public class WatermelonDBBackgroundSync: NSObject {

    // MARK: - State

    private static var _taskId: String?
    private static var _enabled = false
    private static var _intervalMinutes: Int = 15
    private static var _requiresNetwork = true

    // MARK: - Registration (call from AppDelegate before RN init)

    /// Register the BGTask identifier with the system.
    /// Must be called in `application(_:didFinishLaunchingWithOptions:)` before the app finishes launching.
    @objc public static func register(taskId: String) {
        _taskId = taskId

        if #available(iOS 13.0, *) {
            BGTaskScheduler.shared.register(
                forTaskWithIdentifier: taskId,
                using: nil
            ) { task in
                handleBackgroundTask(task)
            }
        }
    }

    // MARK: - Configuration (called from JS bridge)

    @objc public static func configure(intervalMinutes: Int, requiresNetwork: Bool) {
        _intervalMinutes = max(1, intervalMinutes)
        _requiresNetwork = requiresNetwork
    }

    // MARK: - Scheduling

    /// Set the mutation queue table name for event-driven background sync.
    @objc public static func setMutationQueueTable(_ tableName: String?) {
        DatabaseBridge.mutationQueueTable = tableName
    }

    @objc public static func schedulePeriodicSync() {
        guard let taskId = _taskId else {
            NSLog("[WatermelonDB][BackgroundSync] Cannot schedule: no taskId registered")
            return
        }
        _enabled = true
        DatabaseBridge.backgroundSyncEnabled = true

        if #available(iOS 13.0, *) {
            let request = BGProcessingTaskRequest(identifier: taskId)
            request.earliestBeginDate = Date(timeIntervalSinceNow: TimeInterval(_intervalMinutes * 60))
            request.requiresNetworkConnectivity = _requiresNetwork
            request.requiresExternalPower = false

            do {
                try BGTaskScheduler.shared.submit(request)
                NSLog("[WatermelonDB][BackgroundSync] Scheduled periodic sync (interval: %d min)", _intervalMinutes)
            } catch {
                NSLog("[WatermelonDB][BackgroundSync] Failed to schedule periodic sync: %@", error.localizedDescription)
            }
        }
    }

    /// Schedule an immediate background sync (e.g., triggered by mutation queue insert).
    @objc public static func scheduleImmediateSync() {
        guard let taskId = _taskId, _enabled else {
            return
        }

        if #available(iOS 13.0, *) {
            let request = BGProcessingTaskRequest(identifier: taskId)
            request.earliestBeginDate = Date(timeIntervalSinceNow: 1) // ASAP
            request.requiresNetworkConnectivity = _requiresNetwork
            request.requiresExternalPower = false

            do {
                try BGTaskScheduler.shared.submit(request)
                NSLog("[WatermelonDB][BackgroundSync] Scheduled immediate sync")
            } catch {
                NSLog("[WatermelonDB][BackgroundSync] Failed to schedule immediate sync: %@", error.localizedDescription)
            }
        }
    }

    @objc public static func cancelAllScheduledTasks() {
        guard let taskId = _taskId else { return }
        _enabled = false
        DatabaseBridge.backgroundSyncEnabled = false

        if #available(iOS 13.0, *) {
            BGTaskScheduler.shared.cancel(taskIdentifierMatching: taskId)
            NSLog("[WatermelonDB][BackgroundSync] Cancelled all scheduled tasks")
        }
    }

    // MARK: - Task Handler

    @available(iOS 13.0, *)
    private static func handleBackgroundTask(_ task: BGTask) {
        NSLog("[WatermelonDB][BackgroundSync] Background task started: %@", task.identifier)

        // Set expiration handler to avoid OS penalty
        task.expirationHandler = {
            NSLog("[WatermelonDB][BackgroundSync] Background task expired")
            task.setTaskCompleted(success: false)
        }

        // Perform pull-only sync via the ObjC++ bridge
        BackgroundSyncBridge.performSync { success, errorMessage in
            if let error = errorMessage, !error.isEmpty {
                NSLog("[WatermelonDB][BackgroundSync] Sync completed with error: %@", error)
            } else {
                NSLog("[WatermelonDB][BackgroundSync] Sync completed successfully")
            }
            task.setTaskCompleted(success: success)

            // Re-schedule if still enabled
            if _enabled {
                schedulePeriodicSync()
            }
        }
    }

    // MARK: - Helpers

    @objc public static var isEnabled: Bool {
        return _enabled
    }
}
