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
