import ErrorAdapter from './error'

describe('ErrorAdapter (reset placeholder) — MOBILE-6149', () => {
  it('throws on imperative method calls while the database is being reset', () => {
    const adapter: any = new ErrorAdapter()

    expect(() => adapter.query()).toThrow(/while the database is being reset/)
    expect(() => adapter.batch()).toThrow(/while the database is being reset/)
    expect(() => adapter.find()).toThrow(/while the database is being reset/)
    expect(() => adapter.unsafeResetDatabase()).toThrow(/while the database is being reset/)
  })

  // Reproduces the remaining MOBILE-6149 crash. WatermelonDB's own query path
  // (Collection._onCacheMiss reads `database.adapter?.underlyingAdapter?._tag`,
  // Database CDC toggles read `this.adapter.underlyingAdapter`) inspects the
  // adapter's *property getters* via optional chaining / `&&`. Optional
  // chaining does NOT short-circuit a getter that THROWS — it only guards
  // null/undefined — so a benign property read during an in-flight
  // unsafeResetDatabase() became a fatal, uncaught crash on Android right after
  // login. Property getters must return `undefined`, not throw; only the
  // imperative methods (find/query/batch/…) represent genuinely-illegal
  // operations during a reset and keep throwing.
  it('does not throw when reading property getters via optional chaining', () => {
    const adapter: any = new ErrorAdapter()

    expect(() => adapter?.underlyingAdapter).not.toThrow()
    expect(adapter?.underlyingAdapter).toBeUndefined()

    // The exact fork-internal reads that crashed (Collection._onCacheMiss):
    expect(() => adapter?.underlyingAdapter?._hybridJSIEnabled).not.toThrow()
    expect(() => adapter?.underlyingAdapter?._tag).not.toThrow()

    expect(() => adapter?.schema).not.toThrow()
    expect(adapter?.schema).toBeUndefined()
    expect(() => adapter?.migrations).not.toThrow()
    expect(adapter?.migrations).toBeUndefined()
  })
})
