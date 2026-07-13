import {Observable, switchMap, distinctUntilChanged, throttleTime} from '../../utils/rx';
import { logError } from '../../utils/common'
import { Unsubscribe } from '../../utils/subscriptions'

import type Query from '../../Query'
import type Model from '../../Model'

let isThrottlingDisabled = false

export function experimentalDisableObserveCountThrottling(): void {
  isThrottlingDisabled = true
}

// Produces an observable version of a query count by re-querying the database
// when any change occurs in any of the relevant Stores.
// TODO: Potential optimizations:
// - increment/decrement counter using matchers on insert/delete

function observeCountThrottled<Record extends Model>(query: Query<Record>): Observable<number> {
  const { collection } = query
  return collection.database.withChangesForTables(query.allTables).pipe(
    throttleTime(250), // Note: this has a bug, but we'll delete it anyway
    // @ts-ignore
    switchMap(
      () =>
        // MOBILE-6149: _fetchCount returns { error } during a reset. The old
        // toPromise() path REJECTED, which errored and permanently TERMINATED
        // this count observable (and the subscribe below has no error handler →
        // uncaught). Map the Result to an observable that LOGS and skips the tick
        // (complete with no emission) on any error, exactly matching the
        // non-throttled path (below) and the fork's established count-observer
        // design: counts are resilient (log + keep the last value + retry on the
        // next change), and errors are never surfaced to the next-only public
        // subscribers. Reset-window errors are the expected case here.
        new Observable<number>((observer) => {
          collection._fetchCount(query, (result) => {
            if ((result as any).error) {
              logError((result as any).error.toString())
              observer.complete()
              return
            }
            observer.next((result as any).value)
            observer.complete()
          })
        }),
    ),
    distinctUntilChanged(),
  )
}

export default function subscribeToCount<Record extends Model>(
  query: Query<Record>,
  isThrottled: boolean,
  subscriber: (arg1: number) => void,
): Unsubscribe {
  if (isThrottled && !isThrottlingDisabled) {
    const observable = observeCountThrottled(query)
    const subscription = observable.subscribe(subscriber)
    return () => subscription.unsubscribe()
  }

  const { collection } = query
  let unsubscribed = false

  let previousCount = -1
  const observeCountFetch = () => {
    collection._fetchCount(query, result => {
      if ((result as any).error) {
        logError((result as any).error.toString())
        return
      }

      const count = (result as any).value
      const shouldEmit = count !== previousCount && !unsubscribed
      previousCount = count
      shouldEmit && subscriber(count)
    })
  }

  const unsubscribe = collection.database.experimentalSubscribe(
    query.allTables,
    observeCountFetch,
    { name: 'subscribeToCount', query, subscriber },
  )
  observeCountFetch()

  return () => {
    unsubscribed = true
    unsubscribe()
  }
}
