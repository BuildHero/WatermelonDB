import { mockDatabase } from '../__tests__/testModels'
import ErrorAdapter from '../adapters/error'
import Query from '../Query'
import * as Q from '../QueryDescription'
import subscribeToSimpleQuery from '../observation/subscribeToSimpleQuery'

// MOBILE-6149 (Android fatal right after login):
// `_onCacheMiss` runs synchronously inside RecordCache._cachedModelForId when a
// record id streamed from a query/sync result isn't in the JS cache — a routine
// event while sync hydrates records. It reads
// `this.database.adapter?.underlyingAdapter?._tag`. During an in-flight
// unsafeResetDatabase() `database.adapter` is the throwing ErrorAdapter
// placeholder, and optional chaining does NOT short-circuit a getter that
// throws, so this benign property read became a fatal, uncaught crash. This
// path is inside WatermelonDB itself, so the app-side getSQLiteUnderlyingAdapter
// guard shipped for the HomeScreen/download-slices sites cannot reach it.
describe('Collection._onCacheMiss during an in-flight database reset — MOBILE-6149', () => {
  it('does not throw a fatal error when a cache miss races unsafeResetDatabase()', () => {
    const { database, tasks } = mockDatabase()

    // Simulate the state WatermelonDB installs for the duration of the native
    // wipe: database.adapter swapped for the throwing ErrorAdapter placeholder.
    database.adapter = new ErrorAdapter()

    // A record id from a query result that isn't cached triggers _onCacheMiss.
    // Before the fix this threw
    //   "Cannot call database.adapter.underlyingAdapter while the database is being reset"
    // synchronously, outside any Promise/try-catch boundary → fatal red screen.
    expect(() =>
      tasks._cache.recordsFromQueryResult(['task_not_in_cache'])
    ).not.toThrow()
  })
})

describe('Collection._fetchRecord re-guards the adapter across the async fallback — MOBILE-6149', () => {
  it('does not query a stale adapter if a reset starts during fetchFromRemote', async () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter

    // First find returns "not found" synchronously so the async
    // fetchFromRemote fallback runs deterministically.
    const findSpy = jest
      .spyOn(realAdapter, 'find')
      .mockImplementation((_table, _id, cb) => cb({ value: null }))

    // Hold fetchFromRemote open so we can start a reset mid-flight.
    let resolveRemote
    jest
      .spyOn(tasks.modelClass, 'fetchFromRemote')
      .mockImplementation(() => new Promise((res) => (resolveRemote = res)))

    let cbResult
    tasks._fetchRecord('missing_id', (r) => {
      cbResult = r
    })

    // First find hit the real adapter exactly once.
    expect(findSpy).toHaveBeenCalledTimes(1)

    // A reset begins while fetchFromRemote is in flight — database.adapter is
    // swapped for the throwing ErrorAdapter placeholder.
    database.adapter = new ErrorAdapter()

    // Let the fallback proceed.
    resolveRemote()
    await Promise.resolve()
    await Promise.resolve()

    // The captured (now stale/torn-down) adapter must NOT be queried a second
    // time; the re-guard surfaces a soft error instead. (Before the fix, the
    // fallback called the captured adapter's find again → findSpy twice.)
    expect(findSpy).toHaveBeenCalledTimes(1)
    expect(cbResult).toBeDefined()
    expect(cbResult.error).toBeInstanceOf(Error)
  })
})

describe('Collection query paths report a caught error (not a false success) during reset — MOBILE-6149', () => {
  it('_fetchQuery surfaces an error result, not an empty-success', () => {
    const { database, tasks } = mockDatabase()
    const query = tasks.query()
    database.adapter = new ErrorAdapter()

    let result
    expect(() => tasks._fetchQuery(query, (r) => (result = r))).not.toThrow()
    // A caught error — NOT { value: [] }, which would be indistinguishable from
    // a genuinely empty table and could mislead callers.
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })

  it('_fetchCount surfaces an error result, not a false 0 count', () => {
    const { database, tasks } = mockDatabase()
    const query = tasks.query()
    database.adapter = new ErrorAdapter()

    let result
    expect(() => tasks._fetchCount(query, (r) => (result = r))).not.toThrow()
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })

  it('_fetchQuery aborts an in-flight query that COMPLETES during a reset (no false-success)', () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter

    // Capture the native completion callback so we can fire it AFTER a reset
    // has started — the query was issued while the adapter was still valid.
    let complete
    jest.spyOn(realAdapter, 'query').mockImplementation((_q, cb) => {
      complete = cb
    })

    let result
    tasks._fetchQuery(tasks.query(), (r) => (result = r))
    expect(typeof complete).toBe('function')

    // A reset begins while the query is in flight.
    database.adapter = new ErrorAdapter()

    // The native query returns during the reset window with raw rows. The
    // result mapper must abort (caught error) rather than resolve a partial /
    // null-holed array that would violate the Query.fetch Record[] contract.
    expect(() => complete({ value: [{ id: 'x', _status: 'synced' }] })).not.toThrow()
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })
})

// The CONFIRMED production crash path: a live query observer refetches
// synchronously when the collection reports an external change (native CDC
// write → SQLITE_UPDATE_HOOK → Database.notify → collection._notifyExternalChange
// → empty changeset → subscribeToSimpleQuery calls _fetchQuery). That refetch
// runs OUTSIDE any Promise/try-catch, so on the pre-fix build the throwing
// underlyingAdapter getter escaped through notify() as a fatal, uncaught crash.
describe('query-observer external-change refetch during a reset — MOBILE-6149 (the fatal notify path)', () => {
  it('does not throw when an external-change refetch races an in-flight reset', async () => {
    const { database, tasks } = mockDatabase()
    await tasks.create((m) => {
      m.name = 'foo'
    })

    const query = new Query(tasks, [Q.where('name', 'foo')])
    const observer = jest.fn()
    const unsubscribe = subscribeToSimpleQuery(query, observer)
    await new Promise(process.nextTick) // let the initial emission settle
    expect(observer).toHaveBeenCalled()

    // Reset window: WatermelonDB swaps database.adapter for the throwing
    // ErrorAdapter placeholder for the duration of unsafeResetDatabase().
    const fetchSpy = jest.spyOn(tasks, '_fetchQuery')
    database.adapter = new ErrorAdapter()

    // An external change triggers the synchronous _fetchQuery refetch. It must
    // fail soft (guard → { error } → observer skips) rather than throw fatally.
    expect(() => tasks._notifyExternalChange()).not.toThrow()
    expect(fetchSpy).toHaveBeenCalledTimes(1) // the refetch path was exercised

    unsubscribe()
  })
})
