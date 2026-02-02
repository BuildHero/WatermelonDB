import Foundation

#if canImport(SocketIO)
import SocketIO

@objc public class SyncSocketClient: NSObject {
    @objc public static let shared = SyncSocketClient()

    @objc public static let statusNotificationName = "WMSyncSocketStatus"
    @objc public static let cdcNotificationName = "WMSyncSocketCDC"

    private var socketManager: SocketManager? = nil
    private var socketClient: SocketIOClient? = nil

    @objc public func initialize(socketUrl: String, debug: Bool) {
        if let existingSocket = socketClient {
            existingSocket.removeAllHandlers()
            existingSocket.disconnect()
        }
        socketClient = nil
        socketManager = nil
        let config: SocketIOClientConfiguration = debug ? [.log(true), .compress] : [.log(false), .compress]
        socketManager = SocketManager(socketURL: URL(string: socketUrl)!, config: config)
        socketClient = socketManager?.defaultSocket

        socketClient?.on(clientEvent: .connect) { [weak self] _, _ in
            self?.postStatus("connected", error: nil)
        }

        socketClient?.on(clientEvent: .disconnect) { [weak self] _, _ in
            self?.postStatus("disconnected", error: nil)
        }

        socketClient?.on(clientEvent: .error) { [weak self] data, _ in
            let message = (data as? [String])?.first
            self?.postStatus("error", error: message)
        }

        socketClient?.on("cdc") { [weak self] _, _ in
            self?.postCDC()
        }
    }

    @objc public func connect() {
        socketClient?.connect()
    }

    @objc public func disconnect() {
        socketClient?.disconnect()
    }

    @objc public func authenticate(_ token: String) {
        socketClient?.emit("sync::auth", ["token": token])
    }

    private func postStatus(_ status: String, error: String?) {
        NotificationCenter.default.post(
            name: Notification.Name(SyncSocketClient.statusNotificationName),
            object: nil,
            userInfo: ["status": status, "error": error as Any]
        )
    }

    private func postCDC() {
        NotificationCenter.default.post(
            name: Notification.Name(SyncSocketClient.cdcNotificationName),
            object: nil,
            userInfo: nil
        )
    }
}
#else
@objc public class SyncSocketClient: NSObject {
    @objc public static let shared = SyncSocketClient()

    @objc public static let statusNotificationName = "WMSyncSocketStatus"
    @objc public static let cdcNotificationName = "WMSyncSocketCDC"

    @objc public func initialize(socketUrl: String, debug: Bool) {
        NotificationCenter.default.post(
            name: Notification.Name(SyncSocketClient.statusNotificationName),
            object: nil,
            userInfo: ["status": "unavailable", "error": "SocketIO not available"]
        )
    }

    @objc public func connect() {}
    @objc public func disconnect() {}
    @objc public func authenticate(_ token: String) {}
}
#endif
