import { TurboModule, TurboModuleRegistry } from 'react-native'

export interface Spec extends TurboModule {
  query(tag: number, table: string, query: string): Record<string, any>[]
  execSqlQuery(tag: number, sql: string, args: Record<string, any>[]): Record<string, any>[]
  importRemoteSlice(
    tag: number, 
    sliceUrl: string
  ): Promise<void>
  configureSync(configJson: string): void
  startSync(reason: string): void
  setSyncPullUrl(pullEndpointUrl: string): void
  getSyncStateJson(): string
  addSyncListener(listener: (eventJson: string) => void): number
  removeSyncListener(listenerId: number): void
  setAuthToken(token: string): void
  clearAuthToken(): void
  setAuthTokenProvider(provider: () => Promise<string> | string): void
  setPushChangesProvider(provider: () => Promise<void> | void): void
  initSyncSocket(socketUrl: string): void
  syncSocketAuthenticate(token: string): void
  syncSocketDisconnect(): void
}

export default TurboModuleRegistry.getEnforcing<Spec>('NativeWatermelonDBModule')
