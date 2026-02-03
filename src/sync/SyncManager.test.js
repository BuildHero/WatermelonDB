const flushPromises = () => new Promise(resolve => setImmediate(resolve))

const makeModule = () => {
  jest.resetModules()

  const nativeSync = {
    configureSync: jest.fn(),
    startSync: jest.fn(),
    getSyncState: jest.fn(() => ({ state: 'idle' })),
    addSyncListener: jest.fn(),
    notifyQueueDrained: jest.fn(),
    setAuthToken: jest.fn(),
    clearAuthToken: jest.fn(),
    initSyncSocket: jest.fn(),
    syncSocketAuthenticate: jest.fn(),
    syncSocketDisconnect: jest.fn(),
  }

  jest.doMock('./nativeSync', () => nativeSync)

  const { SyncManager } = require('./SyncManager')
  return { SyncManager, nativeSync }
}

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
    expect(() => SyncManager.configure({ connectionTag: 1 })).toThrow(
      '[WatermelonDB][Sync] configure requires endpoint or pullUrl.',
    )
    expect(() => SyncManager.configure({ endpoint: 'https://example.com' })).toThrow(
      '[WatermelonDB][Sync] configure requires a numeric connectionTag > 0.',
    )
    expect(() => SyncManager.configure({ endpoint: 'x', connectionTag: 1, authTokenProvider: 'nope' })).toThrow(
      '[WatermelonDB][Sync] authTokenProvider must be a function when provided.',
    )
  })

  it('accepts pullUrl and passes config to native', () => {
    const { SyncManager, nativeSync } = makeModule()
    SyncManager.configure({ pullUrl: 'https://example.com/pull', connectionTag: 2 })
    expect(nativeSync.configureSync).toHaveBeenCalledWith({
      pullUrl: 'https://example.com/pull',
      connectionTag: 2,
    })
  })

  it('uses authTokenProvider and refreshes on auth_required', async () => {
    const { SyncManager, nativeSync } = makeModule()
    let capturedListener
    nativeSync.addSyncListener.mockImplementation(listener => {
      capturedListener = listener
      return jest.fn()
    })

    const provider = jest
      .fn()
      .mockResolvedValueOnce('token-1')
      .mockResolvedValueOnce('token-2')

    SyncManager.configure({
      endpoint: 'https://example.com',
      connectionTag: 1,
      authTokenProvider: provider,
    })

    await flushPromises()
    expect(nativeSync.setAuthToken).toHaveBeenCalledWith('token-1')

    await capturedListener({ type: 'auth_required' })
    await flushPromises()
    expect(nativeSync.setAuthToken).toHaveBeenCalledWith('token-2')
  })

  it('throws if setQueueDrainHandler is called before configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.setQueueDrainHandler(() => {})).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before setQueueDrainHandler.',
    )
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

  it('throws if auth methods are called before configure', () => {
    const { SyncManager } = makeModule()
    expect(() => SyncManager.setAuthToken('token')).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before setAuthToken.',
    )
    expect(() => SyncManager.clearAuthToken()).toThrow(
      '[WatermelonDB][Sync] SyncManager.configure(...) must be called before clearAuthToken.',
    )
  })

  it('executes queue drain handler and notifies native', async () => {
    const { SyncManager, nativeSync } = makeModule()
    let capturedListener
    nativeSync.addSyncListener.mockImplementation(listener => {
      capturedListener = listener
      return jest.fn()
    })

    const handler = jest.fn().mockResolvedValue(undefined)
    SyncManager.configure({ endpoint: 'https://example.com', connectionTag: 1 })
    SyncManager.setQueueDrainHandler(handler)

    await capturedListener({ type: 'drain_queue' })
    await flushPromises()

    expect(handler).toHaveBeenCalledTimes(1)
    expect(nativeSync.notifyQueueDrained).toHaveBeenCalledTimes(1)
  })

  it('replaces existing queue drain handler subscription', async () => {
    const { SyncManager, nativeSync } = makeModule()
    const unsubscribe = jest.fn()
    nativeSync.addSyncListener.mockReturnValue(unsubscribe)

    SyncManager.configure({ endpoint: 'https://example.com', connectionTag: 1 })
    SyncManager.setQueueDrainHandler(() => {})
    SyncManager.setQueueDrainHandler(() => {})

    expect(unsubscribe).toHaveBeenCalledTimes(1)
  })

  it('ignores non drain_queue events for queue drain handler', async () => {
    const { SyncManager, nativeSync } = makeModule()
    let capturedListener
    nativeSync.addSyncListener.mockImplementation(listener => {
      capturedListener = listener
      return jest.fn()
    })

    const handler = jest.fn()
    SyncManager.configure({ endpoint: 'https://example.com', connectionTag: 1 })
    SyncManager.setQueueDrainHandler(handler)

    await capturedListener({ type: 'state', state: 'syncing' })
    await flushPromises()

    expect(handler).not.toHaveBeenCalled()
    expect(nativeSync.notifyQueueDrained).not.toHaveBeenCalled()
  })

  it('does not crash if authTokenProvider throws', async () => {
    const { SyncManager, nativeSync } = makeModule()
    let capturedListener
    nativeSync.addSyncListener.mockImplementation(listener => {
      capturedListener = listener
      return jest.fn()
    })

    const provider = jest.fn(() => {
      throw new Error('boom')
    })

    SyncManager.configure({
      endpoint: 'https://example.com',
      connectionTag: 1,
      authTokenProvider: provider,
    })

    await flushPromises()
    expect(nativeSync.setAuthToken).not.toHaveBeenCalled()

    await capturedListener({ type: 'auth_required' })
    await flushPromises()
    expect(nativeSync.setAuthToken).not.toHaveBeenCalled()
  })

  it('does not set auth token when provider returns empty', async () => {
    const { SyncManager, nativeSync } = makeModule()
    const provider = jest.fn().mockResolvedValue('')
    SyncManager.configure({
      endpoint: 'https://example.com',
      connectionTag: 1,
      authTokenProvider: provider,
    })
    await flushPromises()
    expect(nativeSync.setAuthToken).not.toHaveBeenCalled()
  })

  it('passes socket methods through to native', () => {
    const { SyncManager, nativeSync } = makeModule()
    SyncManager.configure({ endpoint: 'https://example.com', connectionTag: 1 })

    SyncManager.initSocket('wss://example.com')
    SyncManager.authenticateSocket('token')
    SyncManager.disconnectSocket()

    expect(nativeSync.initSyncSocket).toHaveBeenCalledWith('wss://example.com')
    expect(nativeSync.syncSocketAuthenticate).toHaveBeenCalledWith('token')
    expect(nativeSync.syncSocketDisconnect).toHaveBeenCalledWith()
  })

  it('passes start and getState through after configure', () => {
    const { SyncManager, nativeSync } = makeModule()
    nativeSync.getSyncState.mockReturnValue({ state: 'configured' })

    SyncManager.configure({ endpoint: 'https://example.com', connectionTag: 1 })
    SyncManager.start('manual')
    const state = SyncManager.getState()

    expect(nativeSync.startSync).toHaveBeenCalledWith('manual')
    expect(state).toEqual({ state: 'configured' })
  })
})
