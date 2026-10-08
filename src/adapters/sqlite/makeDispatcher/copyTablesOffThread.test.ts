jest.mock('react-native', () => ({
  NativeModules: {
    DatabaseBridge: {
      copyTables: jest.fn(),
      copyTablesSynchronous: jest.fn(),
      copyTablesOffThread: jest.fn(),
    },
  },
  TurboModuleRegistry: { get: () => null },
}))

// eslint-disable-next-line import/first
import { NativeModules } from 'react-native'
// eslint-disable-next-line import/first
import { makeDispatcher } from './index.native'

const bridge = NativeModules.DatabaseBridge
const offThread = bridge.copyTablesOffThread

const flush = () => new Promise((resolve) => setImmediate(resolve))

describe('makeDispatcher copyTables', () => {
  beforeEach(() => {
    bridge.copyTables = jest.fn(() => Promise.resolve(null))
    bridge.copyTablesSynchronous = jest.fn(() => ({ status: 'success', result: null }))
    bridge.copyTablesOffThread = jest.fn(() => Promise.resolve(null))
  })

  afterAll(() => {
    bridge.copyTablesOffThread = offThread
  })

  it('uses copyTablesOffThread on a synchronous connection and reports through the callback', async () => {
    const callback = jest.fn()
    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks', 'projects'], '/tmp/src.db', callback)

    expect(bridge.copyTablesOffThread).toHaveBeenCalledWith(7, ['tasks', 'projects'], '/tmp/src.db')
    expect(bridge.copyTablesSynchronous).not.toHaveBeenCalled()
    expect(callback).not.toHaveBeenCalled()

    await flush()
    expect(callback).toHaveBeenCalledWith({ value: null })
  })

  it('passes a native rejection to the callback as an error', async () => {
    const error = new Error('disk I/O error')
    bridge.copyTablesOffThread = jest.fn(() => Promise.reject(error))
    const callback = jest.fn()

    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', callback)
    await flush()

    expect(callback).toHaveBeenCalledWith({ error })
  })

  it('falls back to the blocking copy when the binary has no copyTablesOffThread', () => {
    delete bridge.copyTablesOffThread
    const callback = jest.fn()

    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', callback)

    expect(bridge.copyTablesSynchronous).toHaveBeenCalledWith(7, ['tasks'], '/tmp/src.db')
    expect(callback).toHaveBeenCalledWith({ value: null })
  })

  it('keeps the existing async copy on an asynchronous connection', async () => {
    const callback = jest.fn()

    makeDispatcher('asynchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', callback)
    await flush()

    expect(bridge.copyTables).toHaveBeenCalledWith(7, ['tasks'], '/tmp/src.db')
    expect(bridge.copyTablesOffThread).not.toHaveBeenCalled()
    expect(callback).toHaveBeenCalledWith({ value: null })
  })
})
