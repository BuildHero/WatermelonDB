# Native Sync (JSI/TurboModule)

This library includes a native sync engine that pulls changes from your backend, applies them via native SQLite, and then invokes your JS `pushChangesProvider` to drain the outbound mutation queue. The JS surface area is intentionally small: configure, start, and subscribe (which returns an unsubscribe function).

## Quick start

```ts
import { SyncManager } from '@nozbe/watermelondb'

SyncManager.configure({
  database,
  pullChangesUrl: 'https://api.example.com/sync/pull',
  authTokenProvider: async () => getLatestAuthToken(),
  pushChangesProvider: async () => {
    await pushChanges()
  },
  timeoutMs: 30000,
  maxRetries: 3,
  retryInitialMs: 1000,
  retryMaxMs: 30000,
})

const unsubscribe = SyncManager.subscribe((event) => {
  if (event.type === 'error') {
    console.warn('[sync] error', event.message)
  }
})
// Call when you no longer need updates.
unsubscribe()

void SyncManager.syncDatabaseAsync('app_launch')
```

### Awaiting a sync

`SyncManager.syncDatabaseAsync(reason)` resolves when the sync reaches `done`, and rejects on `error`.

```ts
try {
  await SyncManager.syncDatabaseAsync('app_launch')
} catch (error) {
  console.warn('[sync] failed', error)
}
```

## Config fields

`SyncManager.configure(config)` accepts a plain object that is JSON-stringified and parsed natively.

- `authTokenProvider` (function, optional): Async/sync function that returns the latest bearer token. Native invokes this via JSI when it needs a token.
- `pushChangesProvider` (function, required): Async/sync function that drains your mutation queue. Resolve for success, reject or throw for failure.
- `database` (Database, recommended): A Watermelon database instance. The sync manager will derive the connection tag internally.
- `adapter` (Adapter, optional): Use this if you don’t want to pass a Database instance. Must expose a numeric `_tag`.
- `connectionTag` (number, optional): Legacy override; kept for backwards compatibility.
- `pullChangesUrl` (string, required): Base pull endpoint URL. Native will append `sequenceId` as a query param when available.
- `socketioUrl` (string, optional): Socket.io base URL (used by `initSocket`).
- `timeoutMs` (number, optional, default `30000`): HTTP timeout.
- `maxRetries` (number, optional, default `3`): Retry count for retriable failures.
- `retryInitialMs` (number, optional, default `1000`): Initial backoff.
- `retryMaxMs` (number, optional, default `30000`): Max backoff.

`SyncManager.syncDatabaseAsync(reason)` starts a sync using the configured `pullChangesUrl`.

### Sequence ID query param

Before each pull, the native engine reads `__watermelon_last_pulled_at` from the `local_storage` table. If present, it appends `sequenceId=<value>` to the pull URL (or replaces an existing `sequenceId` param). This is the same value your backend should treat as the last cursor/ULID for incremental syncs.

### Cursor pagination (`next` / `cursor`)

If your backend returns paginated results, include a `next` field in the pull response. When `next` is present and non-null, native will:

1) apply that page in a single transaction  
2) issue another pull with `cursor=<next>` appended to the URL  

Notes:

- `next` can be a string (assumed already URL-encoded, used as-is) or an object/array. Objects are JSON-stringified and URL-encoded before being sent as `cursor`.
- Pagination continues until `next` is `null`/missing.

## Events

Events are JSON objects parsed from native. Two shapes are used:

### Sync engine events (have `type`)

Common events:

- `{"type":"state","state":"sync_requested|syncing|retry_scheduled|done|auth_required|error"}`
- `{"type":"sync_start","reason":"..." }`
- `{"type":"sync_queued","reason":"..." }`
- `{"type":"phase","phase":"pull|push","attempt":N}`
- `{"type":"http","phase":"pull","status":200}`
- `{"type":"retry_scheduled","attempt":N,"delayMs":1234,"message":"..." }`
- `{"type":"sync_retry","attempt":N }`
- `{"type":"auth_required"}` (native will call the JS authTokenProvider; you don't need to handle this unless you want custom UX)
- `{"type":"error","message":"..." }`

### Socket events (have `status`)

When socket.io is enabled, events look like:

- `{"status":"connected"}`
- `{"status":"disconnected"}`
- `{"status":"error","data":"<message>" }`
- `{"status":"cdc"}`

## Socket.io (optional)

Initialize the socket when you want it. If you pass `socketioUrl` to `configure`, you can call `initSocket()` without arguments:

```ts
SyncManager.initSocket()
// If authTokenProvider is set, initSocket will authenticate automatically.
// Otherwise, call authenticateSocket with your token:
// SyncManager.authenticateSocket('token')
// ...
SyncManager.reconnectSocket()
SyncManager.disconnectSocket()
```

The `cdc` socket event is intended to trigger a sync: subscribe to events and call `SyncManager.start('cdc')` when you receive `{status:'cdc'}`.

## Native API (for reference)

Under the hood, JS calls:

- `configureSync(configJson)`
- `syncDatabaseAsync(reason)`
- `setSyncPullUrl(pullEndpointUrl)`
- `getSyncStateJson()`
- `addSyncListener((eventJson) => ...)`
- `removeSyncListener(id)`
- `setAuthToken(token)`
- `clearAuthToken()`
- `setAuthTokenProvider(provider)`
- `setPushChangesProvider(provider)`
- `initSyncSocket(socketUrl)`
- `syncSocketAuthenticate(token)`
- `syncSocketDisconnect()`
