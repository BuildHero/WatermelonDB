import type { default as Database, NativePullChangeSet } from '../Database'
import { getLastPulledAt } from './impl'
import {
  configureSync as nativeConfigureSync,
  startSync as nativeStartSync,
  syncDatabaseAsync as nativeSyncDatabaseAsync,
  setSyncPullUrl as nativeSetSyncPullUrl,
  getSyncState as nativeGetSyncState,
  addSyncListener as nativeAddSyncListener,
  setAuthToken as nativeSetAuthToken,
  clearAuthToken as nativeClearAuthToken,
  setAuthTokenProvider as nativeSetAuthTokenProvider,
  setPushChangesProvider as nativeSetPushChangesProvider,
  initSyncSocket as nativeInitSyncSocket,
  syncSocketAuthenticate as nativeSyncSocketAuthenticate,
  syncSocketDisconnect as nativeSyncSocketDisconnect,
  importRemoteSlice as nativeImportRemoteSlice,
  cancelSync as nativeCancelSync,
  configureBackgroundSync as nativeConfigureBackgroundSync,
  enableBackgroundSync as nativeEnableBackgroundSync,
  disableBackgroundSync as nativeDisableBackgroundSync,
} from './nativeSync'
import type { BackgroundSyncConfig } from './nativeSync'

export type SyncState = {
  state?: string
  [key: string]: any
}

export type SyncEvent = {
  type?: string
  [key: string]: any
}

export type SyncUnsubscribe = () => void

export type SyncConfig = {
  database?: Database | null
  adapter?: unknown
  connectionTag?: number | null
  pullChangesUrl?: string | null
  socketioUrl?: string | null
  timeoutMs?: number
  maxRetries?: number
  maxAuthRetries?: number
  retryInitialMs?: number
  retryMaxMs?: number
  authTokenProvider?: () => Promise<string> | string
  pushChangesProvider?: () => Promise<void> | void
  backgroundSyncTaskId?: string | null
  backgroundSyncMinIntervalMinutes?: number
  backgroundSyncRequiresNetwork?: boolean
  backgroundSyncMutationQueueTable?: string | null
  [key: string]: any
}

export class SyncManager {
  private static configured = false
  private static connectionTag: number | null = null
  private static pullChangesUrl: string | null = null
  private static socketioUrl: string | null = null
  private static adapter: any | null = null
  private static database: Database | null = null
  private static authTokenProvider: (() => Promise<string> | string) | null = null
  private static jsListeners = new Set<(event: SyncEvent) => void>()

  static configure(config: SyncConfig): void {
    const {
      authTokenProvider,
      pushChangesProvider,
      database,
      adapter,
      connectionTag,
      pullChangesUrl,
      ...rest
    } = config ?? {}

    SyncManager.database = database ?? null

    const resolvedConnectionTag = SyncManager.resolveConnectionTag(connectionTag, database, adapter)
    const resolvedPullChangesUrl = pullChangesUrl ?? null

    const nativeConfig = {
      ...rest,
      connectionTag: resolvedConnectionTag,
      pullEndpointUrl: resolvedPullChangesUrl,
    }

    SyncManager.assertValidConfig(
      config,
      nativeConfig,
      authTokenProvider,
      pushChangesProvider,
      resolvedPullChangesUrl,
    )

    SyncManager.connectionTag = resolvedConnectionTag
    SyncManager.pullChangesUrl = resolvedPullChangesUrl
    SyncManager.socketioUrl = config.socketioUrl ?? null
    SyncManager.adapter = SyncManager.resolveAdapter(database, adapter)
    SyncManager.authTokenProvider = authTokenProvider && typeof authTokenProvider === 'function'
      ? authTokenProvider
      : null

    nativeConfigureSync(nativeConfig)

    if (config.backgroundSyncTaskId) {
      nativeConfigureBackgroundSync({
        taskId: config.backgroundSyncTaskId,
        intervalMinutes: config.backgroundSyncMinIntervalMinutes ?? 15,
        requiresNetwork: config.backgroundSyncRequiresNetwork ?? true,
        mutationQueueTable: config.backgroundSyncMutationQueueTable ?? null,
      })
    }

    SyncManager.configured = true

    if (authTokenProvider && typeof authTokenProvider === 'function') {
      SyncManager.setAuthTokenProvider(authTokenProvider)
    }

    if (pushChangesProvider && typeof pushChangesProvider === 'function') {
      SyncManager.setPushChangesProvider(pushChangesProvider)
    }

    if (SyncManager.socketioUrl) {
      SyncManager.initSocket()
    }
  }

  static syncDatabaseAsync(reason: string): Promise<void> {
    SyncManager.assertConfigured('syncDatabaseAsync')
    return SyncManager.refreshPullChangesUrlFromSequenceId()
      .catch(() => { })
      .then(() => nativeSyncDatabaseAsync(reason))
      .then((resultJson) => SyncManager.applyNativePullResult(resultJson))
  }

  // MOBILE-6276: the native pull applies rows straight to SQLite, bypassing Database.batch, so the JS
  // layer must be told what changed or it serves pre-sync values (stale UI until app restart). Native
  // resolves an envelope { changeset: { table: { upserted, deleted } }, error: string | null } on
  // EVERY outcome — success, a pull failure, or a push failure. Because the pulled rows are already
  // committed to SQLite (and the cursor advanced) before the push runs, the changeset must refresh the
  // JS layer even when the push leg fails and the sync ultimately rejects — otherwise those rows stay
  // JS-stale until a later server change or app restart. So: apply the changeset first (through
  // Database.applyNativePullChanges → the standard notify(changeSet) reconciliation), THEN rethrow any
  // error so the caller still sees the failure. Falls back to a coarse notify(allTables) only when
  // there is no usable changeset (a native build predating the envelope resolves undefined).
  private static applyNativePullResult(resultJson: string | void): Promise<void> {
    const { changeSet, error } = SyncManager.parsePullResult(resultJson)
    return SyncManager.refreshFromChangeSet(changeSet).then(() => {
      if (error) {
        throw new Error(error)
      }
    })
  }

  private static refreshFromChangeSet(changeSet: NativePullChangeSet | null): Promise<void> {
    const database = SyncManager.database
    if (!database || !database.schema || typeof database.notify !== 'function') {
      return Promise.resolve()
    }
    if (changeSet && typeof database.applyNativePullChanges === 'function') {
      // applyNativePullChanges bounds its work by the JS cache size (not the pull size), so it is
      // safe to run for pulls of any size — no id-count cap needed.
      return database.applyNativePullChanges(changeSet).catch((refreshError: unknown) => {
        // A refresh failure only means stale UI, never data loss (the rows are in SQLite). Surface it
        // (visible during the Nitro rollout) rather than swallow it.
        SyncManager.emit({ type: 'error', message: 'native_pull_refresh_failed', error: String(refreshError) })
      })
    }
    // Fallback: no usable changeset (a native build predating the envelope resolves undefined) →
    // coarse table-level notify so query observers still re-run.
    const tables = Object.keys(database.schema.tables)
    if (tables.length > 0) {
      database.notify(tables)
    }
    return Promise.resolve()
  }

  private static parsePullResult(resultJson: string | void): {
    changeSet: NativePullChangeSet | null
    error: string | null
  } {
    if (typeof resultJson !== 'string' || resultJson.length === 0) {
      return { changeSet: null, error: null }
    }
    let parsed: any
    try {
      parsed = JSON.parse(resultJson)
    } catch {
      return { changeSet: null, error: null }
    }
    if (!parsed || typeof parsed !== 'object') {
      return { changeSet: null, error: null }
    }
    // Envelope form { changeset, error } (native + JS ship together, so this is what native sends).
    if ('changeset' in parsed || 'error' in parsed) {
      const cs = parsed.changeset && typeof parsed.changeset === 'object' ? (parsed.changeset as NativePullChangeSet) : null
      const err = typeof parsed.error === 'string' && parsed.error.length > 0 ? parsed.error : null
      return { changeSet: cs, error: err }
    }
    // Backward-compat: a bare changeset object from a pre-envelope native.
    return { changeSet: parsed as NativePullChangeSet, error: null }
  }

  static getState(): SyncState {
    SyncManager.assertConfigured('getState')
    return nativeGetSyncState()
  }

  static subscribe(listener: (event: SyncEvent) => void): SyncUnsubscribe {
    SyncManager.jsListeners.add(listener)
    const unsubscribe = nativeAddSyncListener(listener)
    return () => {
      SyncManager.jsListeners.delete(listener)
      if (typeof unsubscribe === 'function') {
        unsubscribe()
      }
    }
  }

  static withConnectionTag(connectionTag: number): SyncConfig {
    return { connectionTag }
  }

  static setAuthToken(token: string): void {
    SyncManager.assertConfigured('setAuthToken')
    nativeSetAuthToken(token)
  }

  static clearAuthToken(): void {
    SyncManager.assertConfigured('clearAuthToken')
    nativeClearAuthToken()
  }

  static setAuthTokenProvider(provider: () => Promise<string> | string): void {
    SyncManager.assertConfigured('setAuthTokenProvider')
    SyncManager.authTokenProvider = provider
    nativeSetAuthTokenProvider(provider)
  }

  static setPushChangesProvider(provider: () => Promise<void> | void): void {
    SyncManager.assertConfigured('setPushChangesProvider')
    nativeSetPushChangesProvider(provider)
  }

  static initSocket(socketUrl?: string | null): void {
    SyncManager.assertConfigured('initSocket')
    const resolvedUrl = socketUrl ?? SyncManager.socketioUrl
    if (!resolvedUrl || resolvedUrl.trim() === '') {
      throw new Error('[WatermelonDB][Sync] initSocket requires socketUrl in configure or as a parameter.')
    }
    nativeInitSyncSocket(resolvedUrl)
    const provider = SyncManager.authTokenProvider
    if (provider) {
      Promise.resolve()
        .then(() => provider())
        .then((token) => {
          if (typeof token === 'string' && token.trim() !== '') {
            nativeSyncSocketAuthenticate(token)
          }
        })
        .catch((error) => {
          SyncManager.emit({
            type: 'error',
            message: 'socket_auth_token_provider_failed',
            error: error?.message ?? String(error),
          })
        })
    }
  }

  static authenticateSocket(token: string): void {
    SyncManager.assertConfigured('authenticateSocket')
    nativeSyncSocketAuthenticate(token)
  }

  static disconnectSocket(): void {
    SyncManager.assertConfigured('disconnectSocket')
    nativeSyncSocketDisconnect()
  }

  static reconnectSocket(socketUrl?: string | null): void {
    SyncManager.assertConfigured('reconnectSocket')
    nativeSyncSocketDisconnect()
    SyncManager.initSocket(socketUrl)
  }

  static importRemoteSlice(sliceUrl: string): Promise<void> {
    SyncManager.assertConfigured('importRemoteSlice')
    const tag = SyncManager.connectionTag
    if (!tag) {
      throw new Error('[WatermelonDB][Sync] importRemoteSlice requires a configured database or adapter.')
    }
    return nativeImportRemoteSlice(tag, sliceUrl)
  }

  static cancelSync(): void {
    SyncManager.assertConfigured('cancelSync')
    nativeCancelSync()
  }

  static enableBackgroundSync(): void {
    SyncManager.assertConfigured('enableBackgroundSync')
    nativeEnableBackgroundSync()
  }

  static disableBackgroundSync(): void {
    SyncManager.assertConfigured('disableBackgroundSync')
    nativeDisableBackgroundSync()
  }

  private static assertConfigured(method: string): void {
    if (SyncManager.configured) {
      return
    }
    const message = `[WatermelonDB][Sync] SyncManager.configure(...) must be called before ${method}.`
    throw new Error(message)
  }

  private static assertValidConfig(
    rawConfig: SyncConfig | null | undefined,
    config: SyncConfig,
    authTokenProvider: unknown,
    pushChangesProvider?: unknown,
    pullChangesUrl?: unknown,
  ): void {
    if (!rawConfig || typeof rawConfig !== 'object') {
      throw new Error('[WatermelonDB][Sync] SyncManager.configure(...) expects a config object.')
    }
    if (authTokenProvider !== undefined && typeof authTokenProvider !== 'function') {
      throw new Error('[WatermelonDB][Sync] authTokenProvider must be a function when provided.')
    }
    if (typeof pushChangesProvider !== 'function') {
      throw new Error('[WatermelonDB][Sync] pushChangesProvider must be a function.')
    }
    if (typeof pullChangesUrl !== 'string' || pullChangesUrl.trim() === '') {
      throw new Error('[WatermelonDB][Sync] pullChangesUrl must be a non-empty string.')
    }

    const connectionTag = config.connectionTag
    if (typeof connectionTag !== 'number' || !Number.isFinite(connectionTag) || connectionTag <= 0) {
      throw new Error('[WatermelonDB][Sync] configure requires a database/adapter or a numeric connectionTag > 0.')
    }
  }

  private static emit(event: SyncEvent): void {
    SyncManager.jsListeners.forEach((listener) => {
      try {
        listener(event)
      } catch {
        // Avoid breaking other listeners
      }
    })
  }

  private static resolveConnectionTag(
    connectionTag: unknown,
    database: unknown,
    adapter: unknown,
  ): number | null {
    if (typeof connectionTag === 'number' && Number.isFinite(connectionTag) && connectionTag > 0) {
      return connectionTag
    }

    const adapterCandidate = adapter || (database && (database as any).adapter)
    const tag =
      (adapterCandidate && (adapterCandidate as any)._tag) ||
      (adapterCandidate &&
        (adapterCandidate as any).underlyingAdapter &&
        (adapterCandidate as any).underlyingAdapter._tag)
    if (typeof tag === 'number' && Number.isFinite(tag) && tag > 0) {
      return tag
    }

    return null
  }

  private static resolveAdapter(database: unknown, adapter: unknown): any | null {
    return (adapter as any) || (database && (database as any).adapter) || null
  }

  private static buildPullChangesUrl(baseUrl: string, sequenceId: string): string {
    const trimmed = baseUrl.trim()
    if (trimmed === '') {
      return trimmed
    }
    const encoded = encodeURIComponent(sequenceId)
    if (/[?&]sequenceId=/.test(trimmed)) {
      return trimmed.replace(/([?&])sequenceId=[^&]*/, `$1sequenceId=${encoded}`)
    }
    const joiner = trimmed.includes('?') ? '&' : '?'
    return `${trimmed}${joiner}sequenceId=${encoded}`
  }

  private static async refreshPullChangesUrlFromSequenceId(): Promise<void> {
    const baseUrl = SyncManager.pullChangesUrl
    if (!baseUrl) {
      return
    }
    const adapter = SyncManager.adapter
    const database = SyncManager.database
    if (!database) {
      return
    }
    if (!adapter || typeof adapter.getLocal !== 'function') {
      return
    }
    let sequenceId: string | null | undefined = null
    try {
      sequenceId = await getLastPulledAt(database as Database, true) as string
    } catch {
      return
    }
    if (!sequenceId || typeof sequenceId !== 'string' || sequenceId.trim() === '') {
      nativeSetSyncPullUrl(baseUrl)
      return
    }
    nativeSetSyncPullUrl(SyncManager.buildPullChangesUrl(baseUrl, sequenceId))
  }
}
