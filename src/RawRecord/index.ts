/* eslint-disable no-lonely-if */
/* eslint-disable no-self-compare */

import { ColumnName, ColumnSchema, TableSchema } from '../Schema'
import { RecordId, SyncStatus } from '../Model'

import randomId from '../utils/common/randomId'

// Raw object representing a model record, coming from an untrusted source
// (disk, sync, user data). Before it can be used to create a Model instance
// it must be sanitized (with `sanitizedRaw`) into a RawRecord
export type DirtyRaw = any

// These fields are ALWAYS present in records of any collection.
type _RawRecord = {
  id: RecordId
  _status: SyncStatus
  _changed: string
}

// Raw object representing a model record. A RawRecord is guaranteed by the type system
// to be safe to use (sanitied with `sanitizedRaw`):
// - it has exactly the fields described by TableSchema (+ standard fields)
// - every field is exactly the type described by ColumnSchema (string, number, or boolean)
// - … and the same optionality (will not be null unless isOptional: true)
export type RawRecord = _RawRecord

// a number, but not NaN (NaN !== NaN) or Infinity
function isValidNumber(value: any): boolean {
  return typeof value === 'number' && value === value && value !== Infinity && value !== -Infinity
}

// Note: This is performance-critical code
function _setRaw(raw: any, key: string, value: any, columnSchema: ColumnSchema): void {
  const { type, isOptional } = columnSchema

  // If the value is wrong type or invalid, it's set to `null` (if optional) or empty value ('', 0, false)
  if (type === 'string') {
    if (typeof value === 'string') {
      raw[key] = value
    } else {
      raw[key] = isOptional ? null : ''
    }
  } else if (type === 'boolean') {
    if (typeof value === 'boolean') {
      raw[key] = value
    } else if (value === 1 || value === 0) {
      // Exception to the standard rule — because SQLite turns true/false into 1/0
      raw[key] = Boolean(value)
    } else {
      raw[key] = isOptional ? null : false
    }
  } else {
    // type = number
    // Treat NaN and Infinity as null
    if (isValidNumber(value)) {
      raw[key] = value || 0
    } else {
      raw[key] = isOptional ? null : 0
    }
  }
}

function isValidStatus(value: any): boolean {
  return value === 'created' || value === 'updated' || value === 'deleted' || value === 'synced'
}

// Transforms a dirty raw record object into a trusted sanitized RawRecord according to passed TableSchema
export function sanitizedRaw(
  dirtyRaw: DirtyRaw,
  tableSchema: TableSchema,
  prefixedKeys: boolean = false,
  prefix: string | null | undefined = undefined,
): RawRecord {
  const { id, _status, _changed } = !prefixedKeys
    ? dirtyRaw
    : {
        id: dirtyRaw[`${prefix || tableSchema.name}.id`],
        _status: dirtyRaw[`${prefix || tableSchema.name}._status`],
        _changed: dirtyRaw[`${prefix || tableSchema.name}`],
      }

  // This is called with `{}` when making a new record, so we need to set a new ID, status
  // Also: If an existing has one of those fields broken, we're screwed. Safest to treat it as a
  // new record (so that it gets synced)

  // TODO: Think about whether prototypeless objects are a useful mitigation
  // const raw = Object.create(null) // create a prototypeless object
  const raw: Record<string, any> = {}

  if (typeof id === 'string') {
    raw.id = id
    raw._status = isValidStatus(_status) ? _status : 'created'
    raw._changed = typeof _changed === 'string' ? _changed : ''
  } else {
    raw.id = randomId()
    raw._status = 'created'
    raw._changed = ''
  }

  // faster than Object.values on a map
  const columns = tableSchema.columnArray
  for (let i = 0, len = columns.length; i < len; i += 1) {
    const columnSchema = columns[i]
    const key = !prefixedKeys
      ? (columnSchema.name as string)
      : `${prefix || tableSchema.name}.${columnSchema.name}`
    // TODO: Check performance
    const value = Object.prototype.hasOwnProperty.call(dirtyRaw, key) ? dirtyRaw[key] : null
    _setRaw(raw, columnSchema.name, value, columnSchema)
  }

  return raw as any
}

// Modifies passed rawRecord by setting sanitized `value` to `columnName`
// Note: Assumes columnName exists and columnSchema matches the name
export function setRawSanitized(
  rawRecord: RawRecord,
  columnName: ColumnName,
  value: any,
  columnSchema: ColumnSchema,
): void {
  _setRaw(rawRecord, columnName, value, columnSchema)
}

export type NullValue = null | '' | 0 | false

export function nullValue(columnSchema: ColumnSchema): NullValue {
  const { isOptional, type } = columnSchema
  if (isOptional) {
    return null
  } else if (type === 'string') {
    return ''
  } else if (type === 'number') {
    return 0
  } else if (type === 'boolean') {
    return false
  }

  throw new Error(`Unknown type for column schema ${JSON.stringify(columnSchema)}`)
}
