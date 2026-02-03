const makeModule = () => {
  jest.resetModules()

  const nativeSync = {
    configureSync: jest.fn(),
    startSync: jest.fn(),
    setSyncPullUrl: jest.fn(),
    getSyncState: jest.fn(() => ({ state: 'idle' })),
    addSyncListener: jest.fn(),
    setAuthToken: jest.fn(),
    clearAuthToken: jest.fn(),
    setAuthTokenProvider: jest.fn(),
    setPushChangesProvider: jest.fn(),
    initSyncSocket: jest.fn(),
    syncSocketAuthenticate: jest.fn(),
    syncSocketDisconnect: jest.fn(),
    importRemoteSlice: jest.fn(() => Promise.resolve()),
  }

  jest.doMock('./nativeSync', () => nativeSync)

  const { SyncManager } = require('./SyncManager')
  return { SyncManager, nativeSync }
}

const flushMicrotasks = () => new Promise(resolve => setImmediate(resolve))

describe('SyncManager', () => {
  it('throws if start is called before configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.start('test')).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before start.',
    )
  })

  it('validates config in configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.configure(null)).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) expects a config object.',
    )
    expect(() => SyncManager.configure({})).toThrow(
      '[WatermelonDB][Sync] pushChangesProvider must be a function.',
    )
    expect(() =>
      SyncManager.configure({ connectionTag: 1, authTokenProvider: 'nope', pullChangesUrl: 'x' }),
    ).toThrow(
      '[WatermelonDB][Sync] authTokenProvider must be a function when provided.',
    )
    expect(() =>
      SyncManager.configure({ connectionTag: 1, pushChangesProvider: 'nope', pullChangesUrl: 'x' }),
    ).toThrow(
      '[WatermelonDB][Sync] pushChangesProvider must be a function.',
    )
    expect(() =>
      SyncManager.configure({ connectionTag: 0, pushChangesProvider: jest.fn(), pullChangesUrl: 'x' }),
    ).toThrow(
      '[WatermelonDB][Sync] configure requires a database/adapter or a numeric connectionTag > 0.',
    )
    expect(() =>
      SyncManager.configure({ connectionTag: 1, pushChangesProvider: jest.fn(), pullChangesUrl: '' }),
    ).toThrow('[WatermelonDB][Sync] pullChangesUrl must be a non-empty string.')
    expect(() => SyncManager.configure({ connectionTag: 1, pushChangesProvider: jest.fn() })).toThrow(
      '[WatermelonDB][Sync] pullChangesUrl must be a non-empty string.',
    )
  })

  it('passes config to native', () => {
    const { SyncManager, nativeSync } = makeModule()
    const adapter = { _tag: 2 }
    SyncManager.configure({ adapter, pushChangesProvider: jest.fn(), pullChangesUrl: 'https://example.com/pull' })
    expect(nativeSync.configureSync).toHaveBeenCalledWith({
      connectionTag: 2,
      pullEndpointUrl: 'https://example.com/pull',
    })
  })

  it('accepts pullChangesUrl in configure', () => {
    const { SyncManager, nativeSync } = makeModule()
    SyncManager.configure({
      adapter: { _tag: 1 },
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })
    expect(nativeSync.configureSync).toHaveBeenCalledWith({
      connectionTag: 1,
      pullEndpointUrl: 'https://example.com/pull',
    })
  })


  it('uses authTokenProvider and registers with native', async () => {
    const { SyncManager, nativeSync } = makeModule()
    const provider = jest.fn().mockResolvedValue('token-1')

    SyncManager.configure({
      adapter: { _tag: 1 },
      authTokenProvider: provider,
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })

    expect(nativeSync.setAuthTokenProvider).toHaveBeenCalledWith(provider)
  })

  it('uses pushChangesProvider and registers with native', () => {
    const { SyncManager, nativeSync } = makeModule()
    const provider = jest.fn().mockResolvedValue(undefined)
    SyncManager.configure({
      adapter: { _tag: 1 },
      pushChangesProvider: provider,
      pullChangesUrl: 'https://example.com/pull',
    })
    expect(nativeSync.setPushChangesProvider).toHaveBeenCalledWith(provider)
  })

  it('throws if socket methods are called before configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.initSocket('wss://example.com')).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before initSocket.',
    )
    expect(() => SyncManager.authenticateSocket('token')).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before authenticateSocket.',
    )
    expect(() => SyncManager.disconnectSocket()).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before disconnectSocket.',
    )
  })

  it('throws if importRemoteSlice is called before configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.importRemoteSlice('https://example.com/slice')).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before importRemoteSlice.',
    )
  })

  it('throws if auth methods are called before configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.setAuthToken('token')).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before setAuthToken.',
    )
    expect(() => SyncManager.clearAuthToken()).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before clearAuthToken.',
    )
  })

  it('starts sync after configure', async () => {
    const { SyncManager, nativeSync } = makeModule()
    SyncManager.configure({
      adapter: { _tag: 1 },
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })
    SyncManager.start('manual')
    await flushMicrotasks()
    expect(nativeSync.startSync).toHaveBeenCalledWith('manual')
  })

  it('passes socket methods through to native', () => {
    const { SyncManager, nativeSync } = makeModule()
    SyncManager.configure({
      adapter: { _tag: 1 },
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })

    SyncManager.initSocket('wss://example.com')
    SyncManager.authenticateSocket('token')
    SyncManager.disconnectSocket()

    expect(nativeSync.initSyncSocket).toHaveBeenCalledWith('wss://example.com')
    expect(nativeSync.syncSocketAuthenticate).toHaveBeenCalledWith('token')
    expect(nativeSync.syncSocketDisconnect).toHaveBeenCalledWith()
  })

  it('passes start and getState through after configure', async () => {
    const { SyncManager, nativeSync } = makeModule()
    nativeSync.getSyncState.mockReturnValue({ state: 'configured' })

    SyncManager.configure({
      adapter: { _tag: 1 },
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })
    SyncManager.start('manual')
    await flushMicrotasks()
    const state = SyncManager.getState()

    expect(nativeSync.startSync).toHaveBeenCalledWith('manual')
    expect(state).toEqual({ state: 'configured' })
  })

  it('updates pullChangesUrl with sequenceId before start', async () => {
    const { SyncManager, nativeSync } = makeModule()
    const adapter = { _tag: 1, getLocal: jest.fn().mockResolvedValue('seq-123') }
    SyncManager.configure({
      adapter,
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })

    SyncManager.start('manual')
    await flushMicrotasks()

    expect(nativeSync.setSyncPullUrl).toHaveBeenCalledWith('https://example.com/pull?sequenceId=seq-123')
    expect(nativeSync.startSync).toHaveBeenCalledWith('manual')
  })

  it('routes importRemoteSlice through to native', async () => {
    const { SyncManager, nativeSync } = makeModule()
    SyncManager.configure({
      adapter: { _tag: 7 },
      pushChangesProvider: jest.fn(),
      pullChangesUrl: 'https://example.com/pull',
    })

    await SyncManager.importRemoteSlice('https://example.com/slice')
    expect(nativeSync.importRemoteSlice).toHaveBeenCalledWith(7, 'https://example.com/slice')
  })
})
