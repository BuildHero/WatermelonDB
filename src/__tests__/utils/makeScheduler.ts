// eslint-disable-next-line
import {TestScheduler} from 'rxjs/internal/testing/TestScheduler';
import type { /* ColdObservable, HotObservable */ Observable } from 'rxjs'
import { ColdObservable } from 'rxjs/internal/testing/ColdObservable';

type HotObservable<T> = Observable<T>;

class WatermelonTestScheduler extends TestScheduler {
  cold<T>(marbles: string, values?: any, error?: any): ColdObservable<T> {
    return this.createColdObservable(marbles, values, error)
  }

  hot<T>(marbles: string, values?: any, error?: any): HotObservable<T> {
    return this.createHotObservable(marbles, values, error)
  }
}

export default function makeScheduler(): WatermelonTestScheduler {
  return new WatermelonTestScheduler((actual: any, expected: any) => {
    expect(actual).toEqual(expected)
  });
}
