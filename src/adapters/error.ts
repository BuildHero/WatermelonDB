// @ts-nocheck

/* eslint-disable getter-return */

// Placeholder adapter installed on `database.adapter` for the duration of
// `unsafeResetDatabase()` (see Database/index.ts) to catch code that touches
// the adapter while the native database is being wiped.
//
// Imperative METHODS (find/query/batch/getLocal/…) throw: issuing a real
// database operation mid-reset is genuinely illegal and should fail loudly.
//
// Property GETTERS (underlyingAdapter/schema/migrations) return `undefined`
// instead of throwing. Callers inspect these defensively via optional chaining
// / `&&` — both in the app and inside WatermelonDB's own query path (e.g.
// Collection._onCacheMiss reads `database.adapter?.underlyingAdapter?._tag`,
// Database CDC toggles read `this.adapter.underlyingAdapter`). Optional
// chaining does NOT short-circuit a getter that throws, so a throwing getter
// turned a benign property read during an in-flight reset into a fatal,
// uncaught crash (MOBILE-6149: Android fatal right after login). Returning
// `undefined` keeps those nullish-safe reads working — they simply observe
// "no adapter available while resetting" and no-op.

const throwError = (name: string) => {
  throw new Error(`Cannot call database.adapter.${name} while the database is being reset`)
}

export default class ErrorAdapter {
  constructor() {
    ;[
      'find',
      'query',
      'count',
      'batch',
      'getDeletedRecords',
      'destroyDeletedRecords',
      'unsafeResetDatabase',
      'getLocal',
      'setLocal',
      'removeLocal',
      'unsafeSqlQuery',
      'testClone',
    ].forEach(name => {
      this[name] = () => throwError(name)
    })
  }

  get underlyingAdapter(): undefined {
    return undefined
  }

  get schema(): undefined {
    return undefined
  }

  get migrations(): undefined {
    return undefined
  }
}
