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

  it('_fetchRecord aborts an in-flight find that COMPLETES during a reset (no cache repopulation)', () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter

    let complete
    jest.spyOn(realAdapter, 'find').mockImplementation((_table, _id, cb) => {
      complete = cb
    })

    let result
    tasks._fetchRecord('rec_1', (r) => (result = r))
    expect(typeof complete).toBe('function')

    // A reset begins while the find is in flight.
    database.adapter = new ErrorAdapter()

    // The native find returns a pre-reset raw record during the reset window.
    // It must abort (caught error) rather than build+cache a Model that would
    // repopulate the cache the reset is wiping.
    expect(() =>
      complete({ value: { id: 'rec_1', _status: 'synced' } })
    ).not.toThrow()
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
    expect(tasks._cache.get('rec_1')).toBeFalsy()
  })
})

// These exercise the CANONICAL reset signals (_isBeingReset + _resetCount epoch,
// the same signals the sync engine's isSameDatabase() trusts) directly — not the
// !underlyingAdapter proxy. The epoch case is the subtle one: a read that begins
// before a reset and completes AFTER the entire reset cycle finishes sees a valid
// adapter and _isBeingReset === false, yet must still be aborted (its data is
// pre-reset). An in-flight-only check would miss it.
describe('canonical reset-signal coverage (_isBeingReset + _resetCount epoch) — MOBILE-6149', () => {
  it('_fetchQuery aborts a completion while _isBeingReset is true (adapter still looks valid)', () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter
    let complete
    jest.spyOn(realAdapter, 'query').mockImplementation((_q, cb) => (complete = cb))

    let result
    tasks._fetchQuery(tasks.query(), (r) => (result = r))

    // Reset in flight, adapter not yet swapped (the pre-swap window).
    database._isBeingReset = true

    expect(() => complete({ value: [{ id: 'x', _status: 'synced' }] })).not.toThrow()
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })

  it('_fetchQuery aborts a query that COMPLETES AFTER a full reset cycle (epoch guard)', () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter
    let complete
    jest.spyOn(realAdapter, 'query').mockImplementation((_q, cb) => (complete = cb))

    let result
    tasks._fetchQuery(tasks.query(), (r) => (result = r))

    // An ENTIRE reset cycle completed: adapter restored (valid), _isBeingReset
    // back to false — only the epoch advanced.
    database._resetCount += 1

    expect(() => complete({ value: [{ id: 'x', _status: 'synced' }] })).not.toThrow()
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })

  it('_fetchCount aborts a completion during a reset (no stale count)', () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter
    let complete
    jest.spyOn(realAdapter, 'count').mockImplementation((_q, cb) => (complete = cb))

    let result
    tasks._fetchCount(tasks.query(), (r) => (result = r))
    database._isBeingReset = true

    expect(() => complete({ value: 42 })).not.toThrow()
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })

  it('unsafeFetchRecordsWithSQL aborts if a reset races the await (no cache repopulation)', async () => {
    const { database, tasks } = mockDatabase()
    database.adapter.unsafeSqlQuery = jest
      .fn()
      .mockResolvedValue([{ id: 'rec_1', _status: 'synced' }])

    const pending = tasks.unsafeFetchRecordsWithSQL('SELECT * FROM mock_tasks')
    // A full reset cycle completed while the SQL query was in flight.
    database._resetCount += 1

    await expect(pending).rejects.toThrow(/resetting/)
    expect(tasks._cache.get('rec_1')).toBeFalsy()
  })

  it('_fetchRecord aborts a CACHE HIT during a reset (no stale cached record)', async () => {
    const { database, tasks } = mockDatabase()
    const rec = await tasks.create((m) => {
      m.name = 'x'
    })
    expect(tasks._cache.get(rec.id)).toBeTruthy() // it's cached

    // Reset in flight — the cache still holds the pre-reset record.
    database._isBeingReset = true

    let result
    tasks._fetchRecord(rec.id, (r) => (result = r))
    // Aborted, NOT served from cache as { value: rec }.
    expect(result.error).toBeInstanceOf(Error)
    expect(result.value).toBeUndefined()
  })

  it('_fetchQuery issues no adapter traffic if a reset is in flight (pre-swap window)', () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter
    const querySpy = jest.spyOn(realAdapter, 'query')

    // Reset in flight, adapter NOT yet swapped — still the real adapter.
    database._isBeingReset = true

    let result
    tasks._fetchQuery(tasks.query(), (r) => (result = r))

    expect(result.error).toBeInstanceOf(Error)
    expect(querySpy).not.toHaveBeenCalled() // no traffic to a resetting DB
  })

  it('_fetchRecord issues no second-find traffic if a reset begins during fetchFromRemote (pre-swap)', async () => {
    const { database, tasks } = mockDatabase()
    const realAdapter = database.adapter.underlyingAdapter

    // First find → not found, so the async fetchFromRemote fallback runs.
    const findSpy = jest
      .spyOn(realAdapter, 'find')
      .mockImplementation((_t, _i, cb) => cb({ value: null }))

    let resolveRemote
    jest
      .spyOn(tasks.modelClass, 'fetchFromRemote')
      .mockImplementation(() => new Promise((res) => (resolveRemote = res)))

    let result
    tasks._fetchRecord('missing_1', (r) => (result = r))
    expect(findSpy).toHaveBeenCalledTimes(1)

    // Reset begins DURING fetchFromRemote, adapter NOT yet swapped (still real).
    // The old re-guard only checked !adapterAfterFetch and would have issued a
    // second find (traffic) against the resetting DB.
    database._isBeingReset = true

    resolveRemote()
    await Promise.resolve()
    await Promise.resolve()

    expect(findSpy).toHaveBeenCalledTimes(1) // no second find
    expect(result.error).toBeInstanceOf(Error)
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
