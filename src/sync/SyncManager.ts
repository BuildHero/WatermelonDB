import type Database from '../Database'
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
} from './nativeSync'

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
