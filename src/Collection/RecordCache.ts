import logError from '../utils/common/logError';
import invariant from '../utils/common/invariant'

import type Model from '../Model'
import type { RecordId } from '../Model'
import type { CachedQueryResult } from '../adapters/type'
import type { TableName } from '../Schema'
import type { RawRecord } from '../RawRecord'
import { sanitizedRaw } from '../RawRecord'

type Instantiator<T> = (arg1: RawRecord) => T;

export default class RecordCache<Record extends Model> {
  map: Map<RecordId, Record> = new Map()

  tableName: TableName<Record>

  recordInsantiator: Instantiator<Record>

  queryFunc: (arg1: RecordId) => void

  version: number

  constructor(
    tableName: TableName<Record>,
    recordInsantiator: Instantiator<Record>,
    queryFunc: (arg1: RecordId) => void,
    initialVersion: number = 0,
  ) {
    this.tableName = tableName
    this.recordInsantiator = recordInsantiator
    this.queryFunc = queryFunc
    this.version = initialVersion
  }

  get(id: RecordId): Record | null | undefined {
    const record = this.map.get(id)

    if (record && (record as any)._cacheVersion < this.version) {
      this.map.delete(id)
      return undefined
    }

    return record
  }

  add(record: Record): void {
    ;(record as any)._cacheVersion = this.version
    this.map.set(record.id, record)
  }

  delete(record: Record): void {
    this.map.delete(record.id)
  }

  unsafeClear(): void {
    this.map = new Map()
  }

  recordsFromQueryResult(result: CachedQueryResult): (Record | null | undefined)[] {
    return result.map(res => this.recordFromQueryResult(res))
  }

  recordFromQueryResult(result: RecordId | RawRecord): Record | null | undefined {
    if (typeof result === 'string') {
      return this._cachedModelForId(result)
    }

    return this._modelForRaw(result)
  }

  _cachedModelForId(id: RecordId): Record | null | undefined {
    const record = this.map.get(id)

    if (!record && !this.queryFunc) {
      invariant(
        record,
        `Record ID ${this.tableName}#${id} was sent over the bridge, but it's not cached`,
      )
    }

    if (!record && this.queryFunc) {
      const data = this.queryFunc(id) as any;

      if (!data) {
        logError(`Record ID ${this.tableName}#${id} was sent over the bridge, but not found`)

        return null;
      }

      return this._modelForRaw(data)
    }

    return record;
  }

  _modelForRaw(raw: RawRecord): Record {
    // Check if already cached (using get() to respect cache versioning)
    const cachedRecord = this.get(raw.id)

    if (cachedRecord) {
      // When native CDC is enabled, we receive full records for data that may already be cached.
      // Update the cached record with fresh data and notify observers so Model.observe() works.
      const sanitized = sanitizedRaw(raw, cachedRecord.collection.schema)

      // Only update and notify if data actually changed
      if (JSON.stringify(cachedRecord._raw) !== JSON.stringify(sanitized)) {
        ;(cachedRecord as any)._raw = sanitized
        cachedRecord._notifyChanged()
      }

      return cachedRecord
    }

    // Return new model
    const newRecord = this.recordInsantiator(raw)
    this.add(newRecord)
    return newRecord
  }
}
