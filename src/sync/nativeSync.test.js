const makeTurboModule = () => ({
  configureSync: jest.fn(),
  startSync: jest.fn(),
  setSyncPullUrl: jest.fn(),
  getSyncStateJson: jest.fn(() => '{"state":"idle"}'),
  addSyncListener: jest.fn(),
  removeSyncListener: jest.fn(),
  setAuthToken: jest.fn(),
  clearAuthToken: jest.fn(),
  setAuthTokenProvider: jest.fn(),
  setPushChangesProvider: jest.fn(),
  initSyncSocket: jest.fn(),
  syncSocketAuthenticate: jest.fn(),
  syncSocketDisconnect: jest.fn(),
  importRemoteSlice: jest.fn(() => Promise.resolve()),
  cancelSync: jest.fn(),
  configureBackgroundSync: jest.fn(),
  enableBackgroundSync: jest.fn(),
  disableBackgroundSync: jest.fn(),
  syncDatabaseAsync: jest.fn(() => Promise.resolve()),
})

const setupModule = (moduleInstance = makeTurboModule()) => {
  jest.resetModules()
  jest.doMock('react-native', () => ({
    TurboModuleRegistry: {
      get: jest.fn(() => moduleInstance),
    },
    TurboModule: class {},
  }))
  // eslint-disable-next-line global-require
  return require('./nativeSync')
}

describe('nativeSync', () => {
  it('throws if native module is missing', () => {
    jest.resetModules()
    jest.doMock('react-native', () => ({
      TurboModuleRegistry: {
        get: jest.fn(() => null),
      },
      TurboModule: class {},
    }))
    const nativeSync = require('./nativeSync')
    expect(() => nativeSync.configureSync({})).toThrow('[WatermelonDB][Sync] NativeWatermelonDBModule not available')
  })

  it('configures sync with JSON string', () => {
    const moduleInstance = makeTurboModule()
    const nativeSync = setupModule(moduleInstance)
    nativeSync.configureSync({ connectionTag: 1 })
    expect(moduleInstance.configureSync).toHaveBeenCalledWith(
      JSON.stringify({ connectionTag: 1 }),
    )
  })

  it('parses getSyncState JSON and handles invalid JSON', () => {
    const moduleInstance = makeTurboModule()
    moduleInstance.getSyncStateJson.mockReturnValue('{"state":"ok"}')
    const nativeSync = setupModule(moduleInstance)
    expect(nativeSync.getSyncState()).toEqual({ state: 'ok' })

    moduleInstance.getSyncStateJson.mockReturnValue('not-json')
    expect(nativeSync.getSyncState()).toEqual({})
  })

  it('wraps addSyncListener and unregisters', () => {
    const moduleInstance = makeTurboModule()
    let capturedListener
    moduleInstance.addSyncListener.mockImplementation(listener => {
      capturedListener = listener
      return 7
    })
    const nativeSync = setupModule(moduleInstance)

    const listener = jest.fn()
    const unsubscribe = nativeSync.addSyncListener(listener)

    capturedListener('{"type":"state","state":"syncing"}')
    expect(listener).toHaveBeenCalledWith({ type: 'state', state: 'syncing' })

    unsubscribe()
    expect(moduleInstance.removeSyncListener).toHaveBeenCalledWith(7)
  })

  it('passes through remaining methods', () => {
    const moduleInstance = makeTurboModule()
    const nativeSync = setupModule(moduleInstance)

    nativeSync.startSync('reason')
    nativeSync.setSyncPullUrl('https://example.com/pull')
    nativeSync.setAuthToken('token')
    nativeSync.clearAuthToken()
    nativeSync.setAuthTokenProvider(() => 'token')
    nativeSync.setPushChangesProvider(() => {})
    nativeSync.initSyncSocket('wss://example.com')
    nativeSync.syncSocketAuthenticate('token')
    nativeSync.syncSocketDisconnect()
    nativeSync.importRemoteSlice(3, 'https://example.com/slice')

    expect(moduleInstance.startSync).toHaveBeenCalledWith('reason')
    expect(moduleInstance.setSyncPullUrl).toHaveBeenCalledWith('https://example.com/pull')
    expect(moduleInstance.setAuthToken).toHaveBeenCalledWith('token')
    expect(moduleInstance.clearAuthToken).toHaveBeenCalledWith()
    expect(moduleInstance.setAuthTokenProvider).toHaveBeenCalledTimes(1)
    expect(moduleInstance.setPushChangesProvider).toHaveBeenCalledTimes(1)
    expect(moduleInstance.initSyncSocket).toHaveBeenCalledWith('wss://example.com')
    expect(moduleInstance.syncSocketAuthenticate).toHaveBeenCalledWith('token')
    expect(moduleInstance.syncSocketDisconnect).toHaveBeenCalledWith()
    expect(moduleInstance.importRemoteSlice).toHaveBeenCalledWith(3, 'https://example.com/slice')
  })

  it('passes through cancelSync', () => {
    const moduleInstance = makeTurboModule()
    const nativeSync = setupModule(moduleInstance)

    nativeSync.cancelSync()
    expect(moduleInstance.cancelSync).toHaveBeenCalledWith()
  })

  it('passes through enableBackgroundSync and disableBackgroundSync', () => {
    const moduleInstance = makeTurboModule()
    const nativeSync = setupModule(moduleInstance)

    nativeSync.enableBackgroundSync()
    nativeSync.disableBackgroundSync()

    expect(moduleInstance.enableBackgroundSync).toHaveBeenCalledWith()
    expect(moduleInstance.disableBackgroundSync).toHaveBeenCalledWith()
  })

  it('serializes configureBackgroundSync as JSON', () => {
    const moduleInstance = makeTurboModule()
    const nativeSync = setupModule(moduleInstance)

    nativeSync.configureBackgroundSync({
      taskId: 'com.test.sync',
      intervalMinutes: 15,
      requiresNetwork: true,
      mutationQueueTable: 'mutations',
    })

    expect(moduleInstance.configureBackgroundSync).toHaveBeenCalledWith(
      JSON.stringify({
        taskId: 'com.test.sync',
        intervalMinutes: 15,
        requiresNetwork: true,
        mutationQueueTable: 'mutations',
      }),
    )
  })

  it('passes through syncDatabaseAsync', async () => {
    const moduleInstance = makeTurboModule()
    const nativeSync = setupModule(moduleInstance)

    await nativeSync.syncDatabaseAsync('manual')
    expect(moduleInstance.syncDatabaseAsync).toHaveBeenCalledWith('manual')
  })
})
