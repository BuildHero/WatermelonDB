# Native Sync (JSI/TurboModule)

This library includes a native sync engine that pulls changes from your backend, applies them via native SQLite, and then waits for your app to drain its outbound mutation queue. The JS surface area is intentionally small: configure, start, subscribe, and notify when the queue is drained.

## Quick start

```ts
import { SyncManager } from '@nozbe/watermelondb'

SyncManager.configure({
  endpoint: 'https://api.example.com/sync',
  connectionTag: 1,
  authTokenProvider: async () => getLatestAuthToken(),
  timeoutMs: 30000,
  maxRetries: 3,
  retryInitialMs: 1000,
  retryMaxMs: 30000,
})

SyncManager.setQueueDrainHandler(async () => {
  // Drain your mutation queue here (GraphQL mutations, etc.)
  // Must resolve when done so native can mark sync complete.
})

SyncManager.subscribe((event) => {
  if (event.type === 'auth_required') {
    // Refresh token, then:
    SyncManager.setAuthToken('new-token')
  }
  if (event.type === 'error') {
    console.warn('[sync] error', event.message)
  }
})

SyncManager.start('app_launch')
```

## Config fields

`SyncManager.configure(config)` accepts a plain object that is JSON-stringified and parsed natively.

- `endpoint` (string, required): Base sync endpoint. Pull uses `endpoint + "/pull"` unless `pullUrl` is set.
- `pullUrl` (string, optional): Full pull URL override.
- `authToken` (string, optional): Initial bearer token (if you already have one).
- `authTokenProvider` (function, optional): Async/sync function that returns the latest bearer token. Used on configure and when native emits `auth_required`.
- `connectionTag` (number, required): SQLite connection tag used for applying changes.
- `timeoutMs` (number, optional, default `30000`): HTTP timeout.
- `maxRetries` (number, optional, default `3`): Retry count for retriable failures.
- `retryInitialMs` (number, optional, default `1000`): Initial backoff.
- `retryMaxMs` (number, optional, default `30000`): Max backoff.

## Queue draining contract

After a successful pull + apply, native emits a `drain_queue` event and waits. You **must** call `SyncManager.setQueueDrainHandler()` so it can run your queue drain logic and then call `notifyQueueDrained()` internally. If you don't, sync will remain in `waiting_for_queue`.

## Events

Events are JSON objects parsed from native. Two shapes are used:

### Sync engine events (have `type`)

Common events:

- `{"type":"state","state":"sync_requested|syncing|retry_scheduled|waiting_for_queue|done|auth_required|error"}`
- `{"type":"sync_start","reason":"..." }`
- `{"type":"sync_queued","reason":"..." }`
- `{"type":"phase","phase":"pull|drain_queue","attempt":N}`
- `{"type":"http","phase":"pull","status":200}`
- `{"type":"retry_scheduled","attempt":N,"delayMs":1234,"message":"..." }`
- `{"type":"sync_retry","attempt":N }`
- `{"type":"auth_required"}`
- `{"type":"error","message":"..." }`
- `{"type":"drain_queue"}`

### Socket events (have `status`)

When socket.io is enabled, events look like:

- `{"status":"connected"}`
- `{"status":"disconnected"}`
- `{"status":"error","data":"<message>" }`
- `{"status":"cdc"}`

## Socket.io (optional)

```ts
SyncManager.initSocket('https://api.example.com')
SyncManager.authenticateSocket('token')
// ...
SyncManager.disconnectSocket()
```

The `cdc` socket event is intended to trigger a sync: subscribe to events and call `SyncManager.start('cdc')` when you receive `{status:'cdc'}`.

## Native API (for reference)

Under the hood, JS calls:

- `configureSync(configJson)`
- `startSync(reason)`
- `getSyncStateJson()`
- `addSyncListener((eventJson) => ...)`
- `removeSyncListener(id)`
- `notifyQueueDrained()`
- `setAuthToken(token)`
- `clearAuthToken()`
- `initSyncSocket(socketUrl)`
- `syncSocketAuthenticate(token)`
- `syncSocketDisconnect()`
