import { Observable, Subject } from '../utils/rx'
import invariant from '../utils/common/invariant'
import noop from '../utils/fp/noop'
import { ResultCallback, toPromise, mapValue, Result } from '../utils/fp/Result'
import { Unsubscribe } from '../utils/subscriptions'

import Query from '../Query'
import type Database from '../Database'
import type Model from '../Model'
import type { RecordId } from '../Model'
import type { Clause } from '../QueryDescription'
import { TableName, TableSchema } from '../Schema'
import { DirtyRaw, RawRecord } from '../RawRecord'

import RecordCache from './RecordCache'
import { CollectionChangeTypes } from './common'
import logger from '../utils/common/logger'
import encodeQuery from '../adapters/sqlite/encodeQuery'
// @ts-ignore
import { mapToGraph } from './helpers'
import { CachedFindResult } from 'adapters/type'

type CollectionChangeType = 'created' | 'updated' | 'destroyed'

export type CollectionChange<T extends Model> = {
  record: T
  type: CollectionChangeType
}
export type CollectionChangeSet<T extends Model> = CollectionChange<T>[]

export type ModelClass<Record extends Model> = (new (...args: any[]) => Record) & {
  table: TableName<Record>
}

export default class Collection<Record extends Model> {
  database: Database

  modelClass: ModelClass<Record>

  // Emits event every time a record inside Collection changes or is deleted
  // (Use Query API to observe collection changes)
  changes: Subject<CollectionChangeSet<Record>> = new Subject()

  _cache: RecordCache<Record>

  constructor(database: Database, ModelClass: ModelClass<Record>) {
    this.database = database
    this.modelClass = ModelClass
    this._cache = new RecordCache(
      ModelClass.table,
      (raw: RawRecord) => new ModelClass(this, raw),
      this._onCacheMiss.bind(this),
    )
  }

  _onCacheMiss(id: RecordId): Record {
    const hasHybridJSI = this.database.adapter?.underlyingAdapter?._hybridJSIEnabled
    const tag = this.database.adapter?.underlyingAdapter?._tag

    if (!hasHybridJSI) {
      invariant(id, `Record ID ${this.table}#${id} was sent over the bridge, but it's not cached`)
    }

    logger.log(
      `Record ID ${this.table}#${id} was sent over the bridge, but it's not cached. Refetching...`,
    )

    // @ts-ignore
    if (!global.WatermelonDB) {
      invariant(
        false,
        `WatermelonDB Turbo Module not available for cache miss on ${this.table}#${id}`,
      )
    }

    // @ts-ignore
    return global.WatermelonDB.execSqlQuery(
      tag,
      `SELECT * FROM ${this.table} WHERE id = ? LIMIT 1`,
      [id],
    )?.[0]
  }

  get db(): Database {
    return this.database
  }

  // Finds a record with the given ID
  // Promise will reject if not found
  async find(id: RecordId): Promise<Record> {
    return toPromise((callback) => this._fetchRecord(id, callback))
  }

  // Finds the given record and starts observing it
  // (with the same semantics as when calling `model.observe()`)
  findAndObserve(id: RecordId): Observable<Record> {
    return Observable.create((observer: any) => {
      let unsubscribe = null as any
      let unsubscribed = false
      this._fetchRecord(id, (result) => {
        if ((result as any).value) {
          const record = (result as any).value
          observer.next(record)
          unsubscribe = record.experimentalSubscribe((isDeleted: any) => {
            if (!unsubscribed) {
              isDeleted ? observer.complete() : observer.next(record)
            }
          })
        } else {
          observer.error((result as any).error)
        }
      })
      return () => {
        unsubscribed = true
        unsubscribe && unsubscribe()
      }
    })
  }

  // Query records of this type
  query(...clauses: Clause[]): Query<Record> {
    return new Query(this, clauses)
  }

  // Creates a new record in this collection
  // Pass a function to set attributes of the record.
  //
  // Example:
  // collections.get(Tables.tasks).create(task => {
  //   task.name = 'Task name'
  // })
  async create(recordBuilder: (arg1: Record) => void = noop): Promise<Record> {
    this.database._ensureInAction(
      `Collection.create() can only be called from inside of an Action. See docs for more details.`,
    )

    const record = this.prepareCreate(recordBuilder)
    await this.database.batch(record)
    return record
  }

  // Prepares a new record in this collection
  // Use this to batch-create multiple records
  prepareCreate(recordBuilder: (arg1: Record) => void = noop): Record {
    // @ts-ignore
    return this.modelClass._prepareCreate(this, recordBuilder)
  }

  // Prepares a new record in this collection based on a raw object
  // e.g. `{ foo: 'bar' }`. Don't use this unless you know how RawRecords work in WatermelonDB
  // this is useful as a performance optimization or if you're implementing your own sync mechanism
  prepareCreateFromDirtyRaw(dirtyRaw: DirtyRaw): Record {
    // @ts-ignore
    return this.modelClass._prepareCreateFromDirtyRaw(this, dirtyRaw)
  }

  // *** Implementation of Query APIs ***

  async unsafeFetchRecordsWithSQL(sql: string): Promise<(Record | null | undefined)[]> {
    const { adapter } = this.database
    invariant(
      typeof adapter.unsafeSqlQuery === 'function',
      'unsafeFetchRecordsWithSQL called on database that does not support SQL',
    )
    // @ts-ignore
    const rawRecords = await adapter.unsafeSqlQuery(this.modelClass.table, sql)

    return this._cache.recordsFromQueryResult(rawRecords)
  }

  // *** Implementation details ***

  get table(): TableName<Record> {
    return this.modelClass.table
  }

  get schema(): TableSchema {
    // @ts-ignore
    return this.database.schema.tables[this.table]
  }

  // See: Query.fetch
  _fetchQuery(query: Query<Record>, callback: ResultCallback<Record[]>): void {
    const serializedQuery = query.serialize()

    const { description, associations } = serializedQuery

    if (description?.eagerJoinTables?.length) {
      return this.database.adapter.underlyingAdapter.execSqlQuery(
        encodeQuery(serializedQuery, false, this.database.schema),
        [],
        (result) =>
          callback(
            mapValue(
              (rawRecords) => mapToGraph(rawRecords, associations, this) as Record[],
              result,
            ),
          ),
      )
    }

    return this.database.adapter.underlyingAdapter.query(query.serialize(), (result) =>
      callback(
        mapValue(
          (rawRecords) => this._cache.recordsFromQueryResult(rawRecords) as Record[],
          result,
        ),
      ),
    )
  }

  // See: Query.fetchCount
  _fetchCount(query: Query<Record>, callback: ResultCallback<number>): void {
    this.database.adapter.underlyingAdapter.count(query.serialize(), callback)
  }

  // Fetches exactly one record (See: Collection.find)
  _fetchRecord(id: RecordId, callback: ResultCallback<Record>): void {
    if (typeof id !== 'string') {
      callback({ error: new Error(`Invalid record ID ${this.table}#${id}`) })
      return
    }

    const cachedRecord = this._cache.get(id)

    if (cachedRecord) {
      callback({ value: cachedRecord })
      return
    }

    const mapQueryResult = (
      id: RecordId,
      result:
        | Result<CachedFindResult>
        | {
            value: CachedFindResult
          },
    ) => {
      return mapValue((rawRecord) => {
        invariant(rawRecord, `Record ${this.table}#${id} not found`)
        return this._cache.recordFromQueryResult(rawRecord as any)
      }, result)
    }

    this.database.adapter.underlyingAdapter.find(this.table, id, (result) => {
      if (!result || !(result as any).value) {
        logger.log(`Record ${this.table}#${id} not found`)
        // @ts-ignore
        this.modelClass.fetchFromRemote(this.modelClass.table, id).then((_: void) => {
          this.database.adapter.underlyingAdapter.find(this.table, id, (result) => {
            callback(mapQueryResult(id, result) as Result<Record>)
          })
        })
      } else {
        callback(mapQueryResult(id, result) as Result<Record>)
      }
    })
  }

  _applyChangesToCache(operations: CollectionChangeSet<Record>): void {
    operations.forEach(({ record, type }) => {
      // @ts-ignore
      if (type === CollectionChangeTypes.created || type === CollectionChangeTypes.upserted) {
        record._isCommitted = true
        this._cache.add(record)
      } else if (type === CollectionChangeTypes.destroyed) {
        this._cache.delete(record)
      }
    })
  }

  _notify(operations: CollectionChangeSet<Record>): void {
    const collectionChangeNotifySubscribers = ([subscriber]: [any]): void => {
      subscriber(operations)
    }
    // @ts-ignore
    this._subscribers.forEach(collectionChangeNotifySubscribers)
    this.changes.next(operations)

    const collectionChangeNotifyModels = ({
      record,
      type,
    }: {
      record: Record
      type: CollectionChangeType
    }): void => {
      // @ts-ignore
      if (type === CollectionChangeTypes.updated || type === CollectionChangeTypes.upserted) {
        record._notifyChanged()
      } else if (type === CollectionChangeTypes.destroyed) {
        record._notifyDestroyed()
      }
    }
    operations.forEach(collectionChangeNotifyModels)
  }

  _subscribers: [(arg1: CollectionChangeSet<Record>) => void, any][] = []

  experimentalSubscribe(
    subscriber: (arg1: CollectionChangeSet<Record>) => void,
    debugInfo?: any,
  ): Unsubscribe {
    const entry = [subscriber, debugInfo]
    this._subscribers.push(entry as any)

    return () => {
      const idx = this._subscribers.indexOf(entry as any)
      idx !== -1 && this._subscribers.splice(idx, 1)
    }
  }

  // See: Database.unsafeClearCaches
  unsafeClearCache(): void {
    this._cache.unsafeClear()
  }
}
