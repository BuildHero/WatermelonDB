import { TurboModule, TurboModuleRegistry } from 'react-native'

export interface Spec extends TurboModule {
  query(tag: number, table: string, query: string): Array<Object>
  execSqlQuery(tag: number, sql: string, args: Array<Object>): Array<Object>
  execSqlQueryOnWriter(tag: number, sql: string, args: Array<Object>): Array<Object>
  importRemoteSlice(
    tag: number,
    sliceUrl: string
  ): Promise<void>
  configureSync(configJson: string): void
  startSync(reason: string): void
  // Resolves the JSON changeset the pull applied ({ "<table>": { "upserted": [...], "deleted": [...] } }).
  syncDatabaseAsync(reason: string): Promise<string>
  setSyncPullUrl(pullEndpointUrl: string): void
  getSyncStateJson(): string
  addSyncListener(listener: (eventJson: string) => void): number
  removeSyncListener(listenerId: number): void
  setAuthToken(token: string): void
  clearAuthToken(): void
  setAuthTokenProvider(provider: () => Promise<string>): void
  setPushChangesProvider(provider: () => Promise<void>): void
  initSyncSocket(socketUrl: string): void
  syncSocketAuthenticate(token: string): void
  syncSocketDisconnect(): void
  cancelSync(): void
  configureBackgroundSync(configJson: string): void
  enableBackgroundSync(): void
  disableBackgroundSync(): void
  decompressZstd(src: string, dest: string): Promise<void>
}

export default TurboModuleRegistry.getEnforcing<Spec>('NativeWatermelonDBModule')
