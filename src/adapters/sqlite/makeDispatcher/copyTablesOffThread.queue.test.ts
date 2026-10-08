jest.mock('react-native', () => {
  const turbo = {
    query: jest.fn(),
    execSqlQuery: jest.fn(),
    execSqlQueryOnWriter: jest.fn(),
  }
  return {
    __turbo: turbo,
    NativeModules: {
      DatabaseBridge: {
        copyTables: jest.fn(),
        copyTablesSynchronous: jest.fn(),
        copyTablesOffThread: jest.fn(),
        find: jest.fn(),
        findSynchronous: jest.fn(),
        count: jest.fn(),
        countSynchronous: jest.fn(),
        getLocal: jest.fn(),
        getLocalSynchronous: jest.fn(),
        query: jest.fn(),
        querySynchronous: jest.fn(),
        execSqlQuery: jest.fn(),
        execSqlQuerySynchronous: jest.fn(),
      },
    },
    TurboModuleRegistry: { get: () => turbo },
  }
})

// eslint-disable-next-line import/first
import * as ReactNative from 'react-native'
// eslint-disable-next-line import/first
import { makeDispatcher } from './index.native'
// eslint-disable-next-line import/first
import { logger } from '../../../utils/common'

// @ts-ignore
const { NativeModules, __turbo: mockTurbo } = ReactNative
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

describe('makeDispatcher while an off-thread copyTables is in flight', () => {
  let order: string[]

  beforeEach(() => {
    order = []
    bridge.findSynchronous = jest.fn(() => {
      order.push('find')
      return { status: 'success', result: 'record' }
    })
    bridge.countSynchronous = jest.fn(() => {
      order.push('count')
      return { status: 'success', result: 3 }
    })
    bridge.getLocalSynchronous = jest.fn(() => {
      order.push('getLocal')
      return { status: 'success', result: 'cursor' }
    })
    mockTurbo.query.mockImplementation(() => {
      order.push('query')
      return ['id1']
    })
    mockTurbo.execSqlQuery.mockImplementation(() => {
      order.push('execSqlQuery')
      return [{ id: 'id1' }]
    })
  })

  it('holds reads until the copy settles, then runs them in call order', async () => {
    const copy = deferred()
    bridge.copyTablesOffThread = jest.fn(() => copy.promise)
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const copyCallback = jest.fn(() => order.push('copy-callback'))
    const findCallback = jest.fn()
    const queryCallback = jest.fn()
    const getLocalCallback = jest.fn()
    const execCallback = jest.fn()
    const countCallback = jest.fn()

    dispatcher.copyTables(['tasks'], '/tmp/src.db', copyCallback)
    dispatcher.find('tasks', 'id1', findCallback)
    dispatcher.query('tasks', 'select * from tasks', queryCallback)
    dispatcher.getLocal('cursor', getLocalCallback)
    dispatcher.execSqlQuery('select id from tasks', [], execCallback)
    dispatcher.count('select count(*) as count from tasks', countCallback)

    expect(order).toEqual([])
    expect(findCallback).not.toHaveBeenCalled()
    expect(queryCallback).not.toHaveBeenCalled()

    copy.resolve(null)
    await flush()

    expect(order).toEqual(['copy-callback', 'find', 'query', 'getLocal', 'execSqlQuery', 'count'])
    expect(findCallback).toHaveBeenCalledWith({ value: 'record' })
    expect(queryCallback).toHaveBeenCalledWith({ value: ['id1'] })
    expect(getLocalCallback).toHaveBeenCalledWith({ value: 'cursor' })
    expect(execCallback).toHaveBeenCalledWith({ value: [{ id: 'id1' }] })
    expect(countCallback).toHaveBeenCalledWith({ value: 3 })
  })

  it('runs calls made from the copy callback after the reads that were already waiting', async () => {
    const copy = deferred()
    bridge.copyTablesOffThread = jest.fn(() => copy.promise)
    const dispatcher = makeDispatcher('synchronous', 7, 'db')

    dispatcher.copyTables(['tasks'], '/tmp/src.db', () => {
      order.push('copy-callback')
      dispatcher.getLocal('cursor', () => order.push('getLocal-callback'))
    })
    dispatcher.find('tasks', 'id1', () => order.push('find-callback'))

    copy.resolve(null)
    await flush()

    expect(order).toEqual([
      'copy-callback',
      'find',
      'find-callback',
      'getLocal',
      'getLocal-callback',
    ])
  })

  it('releases waiting reads when the copy fails', async () => {
    const copy = deferred()
    bridge.copyTablesOffThread = jest.fn(() => copy.promise)
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const copyCallback = jest.fn()
    const findCallback = jest.fn()
    const error = new Error('disk I/O error')

    dispatcher.copyTables(['tasks'], '/tmp/src.db', copyCallback)
    dispatcher.find('tasks', 'id1', findCallback)

    copy.reject(error)
    await flush()

    expect(copyCallback).toHaveBeenCalledWith({ error })
    expect(findCallback).toHaveBeenCalledWith({ value: 'record' })
  })

  it('keeps running waiting calls when one of their callbacks throws', async () => {
    const copy = deferred()
    bridge.copyTablesOffThread = jest.fn(() => copy.promise)
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const getLocalCallback = jest.fn()
    const loggerError = jest.spyOn(logger, 'error').mockImplementation(() => {})

    dispatcher.copyTables(['tasks'], '/tmp/src.db', jest.fn())
    dispatcher.find('tasks', 'id1', () => {
      throw new Error('subscriber blew up')
    })
    dispatcher.getLocal('cursor', getLocalCallback)

    copy.resolve(null)
    await flush()

    expect(getLocalCallback).toHaveBeenCalledWith({ value: 'cursor' })
    expect(loggerError).toHaveBeenCalledWith(
      '[WatermelonDB][SQLite] callback deferred behind copyTables threw',
      new Error('subscriber blew up'),
    )

    const countCallback = jest.fn()
    dispatcher.count('select count(*) as count from tasks', countCallback)
    expect(countCallback).toHaveBeenCalledWith({ value: 3 })
  })

  it('runs reads immediately once the copy has finished', async () => {
    bridge.copyTablesOffThread = jest.fn(() => Promise.resolve(null))
    const dispatcher = makeDispatcher('synchronous', 7, 'db')

    dispatcher.copyTables(['tasks'], '/tmp/src.db', jest.fn())
    await flush()

    const findCallback = jest.fn()
    dispatcher.find('tasks', 'id1', findCallback)

    expect(findCallback).toHaveBeenCalledWith({ value: 'record' })
  })

  it('does not hold reads behind the blocking fallback copy', () => {
    delete bridge.copyTablesOffThread
    bridge.copyTablesSynchronous = jest.fn(() => ({ status: 'success', result: null }))
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const findCallback = jest.fn()

    dispatcher.copyTables(['tasks'], '/tmp/src.db', jest.fn())
    dispatcher.find('tasks', 'id1', findCallback)

    expect(findCallback).toHaveBeenCalledWith({ value: 'record' })
  })

  it('runs a second copyTables only after the first one settles', async () => {
    const first = deferred()
    const second = deferred()
    bridge.copyTablesOffThread = jest
      .fn()
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise)
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const firstCallback = jest.fn(() => order.push('first-callback'))
    const secondCallback = jest.fn(() => order.push('second-callback'))
    const findCallback = jest.fn(() => order.push('find-callback'))

    dispatcher.copyTables(['tasks'], '/tmp/a.db', firstCallback)
    dispatcher.copyTables(['projects'], '/tmp/b.db', secondCallback)
    dispatcher.find('tasks', 'id1', findCallback)

    expect(bridge.copyTablesOffThread).toHaveBeenCalledTimes(1)

    first.resolve(null)
    await flush()

    expect(bridge.copyTablesOffThread).toHaveBeenCalledTimes(2)
    expect(bridge.copyTablesOffThread).toHaveBeenLastCalledWith(7, ['projects'], '/tmp/b.db')
    expect(findCallback).not.toHaveBeenCalled()

    second.resolve(null)
    await flush()

    expect(order).toEqual(['first-callback', 'second-callback', 'find', 'find-callback'])
  })

  it('reports a synchronous bridge throw and does not leave later calls stuck', () => {
    const error = new Error('bridge exploded')
    bridge.copyTablesOffThread = jest.fn(() => {
      throw error
    })
    const dispatcher = makeDispatcher('synchronous', 7, 'db')
    const copyCallback = jest.fn()
    const findCallback = jest.fn()

    expect(() => dispatcher.copyTables(['tasks'], '/tmp/src.db', copyCallback)).not.toThrow()
    dispatcher.find('tasks', 'id1', findCallback)

    expect(copyCallback).toHaveBeenCalledWith({ error })
    expect(findCallback).toHaveBeenCalledWith({ value: 'record' })
  })
})
