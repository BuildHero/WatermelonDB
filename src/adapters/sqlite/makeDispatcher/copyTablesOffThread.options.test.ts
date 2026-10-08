jest.mock('react-native', () => ({
  NativeModules: {
    DatabaseBridge: {
      copyTables: jest.fn(),
      copyTablesSynchronous: jest.fn(),
      copyTablesOffThread: jest.fn(),
      find: jest.fn(),
      findSynchronous: jest.fn(),
    },
  },
  TurboModuleRegistry: { get: () => null },
}))

// eslint-disable-next-line import/first
import { NativeModules } from 'react-native'
// eslint-disable-next-line import/first
import { makeDispatcher } from './index.native'
// eslint-disable-next-line import/first
import { configureCopyTables } from '../index'

const bridge = NativeModules.DatabaseBridge

const flush = () => new Promise((resolve) => setImmediate(resolve))

const deferred = () => {
  let resolve: (value: any) => void = () => {}
  let reject: (error: any) => void = () => {}
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

describe('configureCopyTables', () => {
  beforeEach(() => {
    bridge.copyTablesSynchronous = jest.fn(() => ({ status: 'success', result: null }))
    bridge.copyTablesOffThread = jest.fn(() => Promise.resolve(null))
    bridge.findSynchronous = jest.fn(() => ({ status: 'success', result: 'record' }))
  })

  afterEach(() => {
    configureCopyTables({ offThread: true, onEvent: null })
  })

  it('forces the blocking copy when offThread is false, without holding other calls', () => {
    configureCopyTables({ offThread: false })
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const copyCallback = jest.fn()
    const findCallback = jest.fn()

    dispatcher.copyTables(['tasks'], '/tmp/src.db', copyCallback)
    dispatcher.find('tasks', 'id1', findCallback)

    expect(bridge.copyTablesSynchronous).toHaveBeenCalledWith(7, ['tasks'], '/tmp/src.db')
    expect(bridge.copyTablesOffThread).not.toHaveBeenCalled()
    expect(copyCallback).toHaveBeenCalledWith({ value: null })
    expect(findCallback).toHaveBeenCalledWith({ value: 'record' })
  })

  it('reads the switch at call time, after the dispatcher was created', async () => {
    const dispatcher = makeDispatcher('synchronous', 7, 'db')

    configureCopyTables({ offThread: false })
    dispatcher.copyTables(['tasks'], '/tmp/src.db', jest.fn())
    expect(bridge.copyTablesSynchronous).toHaveBeenCalledTimes(1)

    configureCopyTables({ offThread: true })
    dispatcher.copyTables(['tasks'], '/tmp/src.db', jest.fn())
    await flush()
    expect(bridge.copyTablesOffThread).toHaveBeenCalledTimes(1)
  })

  it('reports start and end of an off-thread copy with its duration', async () => {
    const onEvent = jest.fn()
    configureCopyTables({ onEvent })
    const copy = deferred()
    bridge.copyTablesOffThread = jest.fn(() => copy.promise)

    makeDispatcher('synchronous', 7, 'db').copyTables(
      ['tasks', 'projects'],
      '/tmp/src.db',
      jest.fn(),
    )
    expect(onEvent).toHaveBeenCalledWith({ type: 'start', mode: 'offThread', tables: 2 })

    copy.resolve(null)
    await flush()

    expect(onEvent).toHaveBeenLastCalledWith({
      type: 'end',
      mode: 'offThread',
      tables: 2,
      durationMs: expect.any(Number),
    })
  })

  it('reports a failed off-thread copy, flagging a cancel', async () => {
    const onEvent = jest.fn()
    configureCopyTables({ onEvent })
    const error = new Error('Off-thread copy cancelled: the database was opened again')
    bridge.copyTablesOffThread = jest.fn(() => Promise.reject(error))

    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', jest.fn())
    await flush()

    expect(onEvent).toHaveBeenLastCalledWith({
      type: 'error',
      mode: 'offThread',
      tables: 1,
      durationMs: expect.any(Number),
      cancelled: true,
      error,
    })
  })

  it.each([
    [
      'a cancel before the copy started',
      'Off-thread copy cancelled: the database was opened again',
      true,
    ],
    [
      'a cancel that interrupted a running INSERT',
      'Error Domain=WatermelonDB Code=9 "interrupted" UserInfo={NSLocalizedDescription=interrupted}',
      true,
    ],
    [
      'a disk-full failure',
      'Error Domain=WatermelonDB Code=13 "database or disk is full" UserInfo={NSLocalizedDescription=database or disk is full}',
      false,
    ],
    ['a code that only starts with 9', 'Error Domain=WatermelonDB Code=90 "unknown"', false],
  ])('flags %s as cancelled: %s', async (_name, message, cancelled) => {
    const onEvent = jest.fn()
    configureCopyTables({ onEvent })
    bridge.copyTablesOffThread = jest.fn(() => Promise.reject(new Error(message)))

    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', jest.fn())
    await flush()

    expect(onEvent).toHaveBeenLastCalledWith(expect.objectContaining({ type: 'error', cancelled }))
  })

  it('reports the blocking copy too', () => {
    const onEvent = jest.fn()
    configureCopyTables({ offThread: false, onEvent })

    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', jest.fn())

    expect(onEvent.mock.calls.map(([event]) => [event.type, event.mode])).toEqual([
      ['start', 'blocking'],
      ['end', 'blocking'],
    ])
  })

  it('keeps copying when the event handler throws', async () => {
    configureCopyTables({
      onEvent: () => {
        throw new Error('logger down')
      },
    })
    const copyCallback = jest.fn()

    makeDispatcher('synchronous', 7, 'db').copyTables(['tasks'], '/tmp/src.db', copyCallback)
    await flush()

    expect(copyCallback).toHaveBeenCalledWith({ value: null })
  })
})
