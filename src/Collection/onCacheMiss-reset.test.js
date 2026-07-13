import { mockDatabase } from '../__tests__/testModels'
import ErrorAdapter from '../adapters/error'

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
})
