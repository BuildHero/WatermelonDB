// Native sync manager JSI/TurboModule wrapper.
// Uses JSON strings to keep TurboModule codegen simple and fast.
// This is a thin layer; native side owns orchestration.
import { TurboModule, TurboModuleRegistry } from 'react-native'

interface NativeSyncModule extends TurboModule {
  configureSync(configJson: string): void
  startSync(reason: string): void
  getSyncStateJson(): string
  addSyncListener(listener: (eventJson: string) => void): number
  removeSyncListener(listenerId: number): void
  notifyQueueDrained(): void
  setAuthToken(token: string): void
  clearAuthToken(): void
  initSyncSocket(socketUrl: string): void
  syncSocketAuthenticate(token: string): void
  syncSocketDisconnect(): void
  importRemoteSlice(tag: number, sliceUrl: string): Promise<void>
}

type SyncConfig = Record<string, any>
type SyncEvent = Record<string, any>

let nativeModule: NativeSyncModule | null = null

function getNativeModule(): NativeSyncModule {
  if (!nativeModule) {
    nativeModule = TurboModuleRegistry.get<NativeSyncModule>('NativeWatermelonDBModule')
    if (!nativeModule) {
      throw new Error('[WatermelonDB][Sync] NativeWatermelonDBModule not available')
    }
  }
  return nativeModule
}

export function configureSync(config: SyncConfig): void {
  const module = getNativeModule()
  module.configureSync(JSON.stringify(config ?? {}))
}

export function startSync(reason: string): void {
  const module = getNativeModule()
  module.startSync(reason ?? 'unknown')
}

export function getSyncState(): SyncEvent {
  const module = getNativeModule()
  try {
    return JSON.parse(module.getSyncStateJson() || '{}')
  } catch {
    return {}
  }
}

export function addSyncListener(listener: (event: SyncEvent) => void): () => void {
  const module = getNativeModule()
  const id = module.addSyncListener((eventJson) => {
    let parsed: SyncEvent = {}
    try {
      parsed = JSON.parse(eventJson || '{}')
    } catch {
      parsed = {}
    }
    listener(parsed)
  })
  return () => module.removeSyncListener(id)
}

export function notifyQueueDrained(): void {
  const module = getNativeModule()
  module.notifyQueueDrained()
}

export function setAuthToken(token: string): void {
  const module = getNativeModule()
  module.setAuthToken(token)
}

export function clearAuthToken(): void {
  const module = getNativeModule()
  module.clearAuthToken()
}

export function initSyncSocket(socketUrl: string): void {
  const module = getNativeModule()
  module.initSyncSocket(socketUrl)
}

export function syncSocketAuthenticate(token: string): void {
  const module = getNativeModule()
  module.syncSocketAuthenticate(token)
}

export function syncSocketDisconnect(): void {
  const module = getNativeModule()
  module.syncSocketDisconnect()
}

export function importRemoteSlice(tag: number, sliceUrl: string): Promise<void> {
  const module = getNativeModule()
  return module.importRemoteSlice(tag, sliceUrl)
}
