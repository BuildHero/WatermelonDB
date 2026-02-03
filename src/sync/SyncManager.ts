import {
  configureSync as nativeConfigureSync,
  startSync as nativeStartSync,
  getSyncState as nativeGetSyncState,
  addSyncListener as nativeAddSyncListener,
  notifyQueueDrained as nativeNotifyQueueDrained,
  setAuthToken as nativeSetAuthToken,
  clearAuthToken as nativeClearAuthToken,
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

export type SyncConfig = Record<string, any>

export class SyncManager {
  private static drainUnsubscribe: (() => void) | null = null
  private static authUnsubscribe: (() => void) | null = null
  private static authTokenProvider: (() => Promise<string> | string) | null = null
  private static configured = false
  private static connectionTag: number | null = null

  static configure(config: SyncConfig): void {
    const { authTokenProvider, ...rest } = config ?? {}
    SyncManager.assertValidConfig(config, rest, authTokenProvider)
    if (authTokenProvider && typeof authTokenProvider === 'function') {
      SyncManager.setAuthTokenProvider(authTokenProvider)
    }
    SyncManager.connectionTag = typeof rest.connectionTag === 'number' ? rest.connectionTag : null
    nativeConfigureSync(rest)
    SyncManager.configured = true
  }

  static start(reason: string): void {
    SyncManager.assertConfigured('start')
    nativeStartSync(reason)
  }

  static getState(): SyncState {
    SyncManager.assertConfigured('getState')
    return nativeGetSyncState()
  }

  static subscribe(listener: (event: SyncEvent) => void): () => void {
    return nativeAddSyncListener(listener)
  }

  static setQueueDrainHandler(handler: () => Promise<void> | void): void {
    SyncManager.assertConfigured('setQueueDrainHandler')
    if (SyncManager.drainUnsubscribe) {
      SyncManager.drainUnsubscribe()
      SyncManager.drainUnsubscribe = null
    }

    SyncManager.drainUnsubscribe = SyncManager.subscribe(async (event) => {
      if (event?.type !== 'drain_queue') {
        return
      }
      try {
        await handler()
      } finally {
        nativeNotifyQueueDrained()
      }
    })
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
    SyncManager.authTokenProvider = provider
    SyncManager.refreshAuthToken()
    if (SyncManager.authUnsubscribe) {
      SyncManager.authUnsubscribe()
      SyncManager.authUnsubscribe = null
    }
    SyncManager.authUnsubscribe = SyncManager.subscribe(async (event) => {
      if (event?.type !== 'auth_required') {
        return
      }
      await SyncManager.refreshAuthToken()
    })
  }

  private static async refreshAuthToken(): Promise<void> {
    const provider = SyncManager.authTokenProvider
    if (!provider) {
      return
    }
    try {
      const token = await provider()
      if (token) {
        nativeSetAuthToken(token)
      }
    } catch {
      // Ignore provider errors; native will stay in auth_required until resolved.
    }
  }

  static initSocket(socketUrl: string): void {
    SyncManager.assertConfigured('initSocket')
    nativeInitSyncSocket(socketUrl)
  }

  static authenticateSocket(token: string): void {
    SyncManager.assertConfigured('authenticateSocket')
    nativeSyncSocketAuthenticate(token)
  }

  static disconnectSocket(): void {
    SyncManager.assertConfigured('disconnectSocket')
    nativeSyncSocketDisconnect()
  }

  static importRemoteSlice(sliceUrl: string): Promise<void> {
    SyncManager.assertConfigured('importRemoteSlice')
    const tag = SyncManager.connectionTag
    if (!tag) {
      throw new Error('[WatermelonDB][Sync] importRemoteSlice requires a configured connectionTag.')
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
  ): void {
    if (!rawConfig || typeof rawConfig !== 'object') {
      throw new Error('[WatermelonDB][Sync] SyncManager.configure(...) expects a config object.')
    }
    if (authTokenProvider !== undefined && typeof authTokenProvider !== 'function') {
      throw new Error('[WatermelonDB][Sync] authTokenProvider must be a function when provided.')
    }
    const pullEndpointUrl = typeof config.pullEndpointUrl === 'string' ? config.pullEndpointUrl.trim() : ''

    if (!pullEndpointUrl) {
      throw new Error('[WatermelonDB][Sync] configure requires pullEndpointUrl.')
    }

    if (config.socketioUrl && typeof config.socketioUrl !== 'string') {
      throw new Error('[WatermelonDB][Sync] socketioUrl must be a string when provided.')
    }

    const connectionTag = config.connectionTag
    if (typeof connectionTag !== 'number' || !Number.isFinite(connectionTag) || connectionTag <= 0) {
      throw new Error('[WatermelonDB][Sync] configure requires a numeric connectionTag > 0.')
    }
  }
}
