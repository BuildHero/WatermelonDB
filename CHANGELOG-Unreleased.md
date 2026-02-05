# Changelog

## Unreleased

### BREAKING CHANGES

### New features

- `database.enableNativeCDC()` now automatically calls `database.notify()` when native code writes to the database. This ensures observers refresh after native sync operations write directly to SQLite. When native CDC is enabled, `batch()` skips its internal `notify()` call to avoid duplicate notifications. Added `database.disableNativeCDC()` for cleanup.

### Performance

### Changes

### Fixes

- Fixed `syncDatabaseAsync()` promises being rejected on `auth_required` even when sync continues after token refresh.
- Fixed `enableNativeCDC()` enabling `batch()` notify suppression when no CDC subscription is available.
- Suppressed "Record ID was sent over the bridge, but it's not cached. Refetching..." warnings when native CDC is enabled, since cache misses are expected when records are created by native sync.
- Fixed `Model.observe()` and `withObservables` not updating when native CDC writes to the database. When queries return full records for already-cached models, the cache now updates the existing model's `_raw` data and calls `_notifyChanged()` to trigger RxJS subscriptions.
- Fixed simple query observables not updating on native CDC changes. `subscribeToSimpleQuery` now refetches the query when it receives an empty changeset (indicating external changes like native CDC).
- Fixed `observeWithColumns` not detecting column changes from native CDC. Now checks all observed records for column changes when receiving an empty changeset.
- Added auth retry limit to SyncEngine. When `authTokenProvider` fails repeatedly, sync now stops after `maxAuthRetries` (default: 3) and emits `auth_failed` event instead of retrying indefinitely.

### Performance

- When native CDC is enabled, queries now return full records instead of just IDs. This eliminates per-record refetch overhead when native sync creates thousands of records that aren't in the JS cache. Added `setCDCEnabled()` method to adapters.

### Internal
