import {all, values, pipe} from 'rambdax';

import { logError, invariant } from '../../utils/common'

import type { Model, Collection, Database } from '../..'
import { RawRecord, DirtyRaw, sanitizedRaw } from '../../RawRecord'
import type { SyncLog, SyncDatabaseChangeSet, SyncConflictResolver } from '../index'

// Returns raw record with naive solution to a conflict based on local `_changed` field
// This is a per-column resolution algorithm. All columns that were changed locally win
// and will be applied on top of the remote version.
export function resolveConflict(local: RawRecord, remote: DirtyRaw): DirtyRaw {
  // We SHOULD NOT have a reference to a `deleted` record, but since it was locally
  // deleted, there's nothing to update, since the local deletion will still be pushed to the server -- return raw as is
  if (local._status === 'deleted') {
    return local
  }

  // mutating code - performance-critical path
  const resolved = {
    // use local fields if remote is missing columns (shouldn't but just in case)
    ...local,
    // Note: remote MUST NOT have a _status of _changed fields (will replace them anyway just in case)
    ...remote,
    id: local.id,
    _status: local._status,
    _changed: local._changed,
  } as const

  // Use local properties where changed
  local._changed?.split(',').forEach((column: string) => {
    // @ts-ignore
    resolved[column] = local[column]
  })

  // Handle edge case
  if (local._status === 'created') {
    logError(
      `[Sync] Server wants client to update record ${local.id}, but it's marked as locally created. This is most likely either a server error or a Watermelon bug (please file an issue if it is!). Will assume it should have been 'synced', and just replace the raw`,
    )
    resolved._status = 'synced'
  }

  return resolved
}

function replaceRaw(record: Model, dirtyRaw: DirtyRaw): void {
  record._raw = sanitizedRaw(dirtyRaw, record.collection.schema)
}

export function prepareCreateFromRaw<T extends Model>(collection: Collection<T>, dirtyRaw: DirtyRaw): T {
  // TODO: Think more deeply about this - it's probably unnecessary to do this check, since it would
  // mean malicious sync server, which is a bigger problem
  invariant(
    !Object.prototype.hasOwnProperty.call(dirtyRaw, '__proto__'),
    'Malicious dirtyRaw detected - contains a __proto__ key',
  )
  const raw = Object.assign({}, dirtyRaw, { _status: 'synced', _changed: '' }) // faster than object spread
  return collection.prepareCreateFromDirtyRaw(raw)
}

export function prepareUpdateFromRaw<T extends Model>(
  record: T,
  updatedDirtyRaw: DirtyRaw,
  log?: SyncLog | null,
  conflictResolver?: SyncConflictResolver,
): T {
  // Note COPY for log - only if needed
  const logConflict = log && !!record._raw._changed
  const logLocal = logConflict ? { ...record._raw } : {};
  const logRemote = logConflict ? { ...updatedDirtyRaw } : {};

  let newRaw = resolveConflict(record._raw, updatedDirtyRaw)

  if (conflictResolver) {
    newRaw = conflictResolver(record.table, record._raw, updatedDirtyRaw, newRaw)
  }

  return record.prepareUpdate(() => {
    replaceRaw(record, newRaw)

    // log resolved conflict - if any
    if (logConflict && log) {
      log.resolvedConflicts = log.resolvedConflicts || []
      log.resolvedConflicts.push({
        local: logLocal,
        remote: logRemote,
        resolved: { ...record._raw },
      })
    }
  });
}

export function prepareMarkAsSynced<T extends Model>(record: T): T {
  const newRaw = Object.assign({}, record._raw, { _status: 'synced', _changed: '' }) // faster than object spread
  return record.prepareUpdate(() => {
    replaceRaw(record, newRaw)
  })
}

export function ensureActionsEnabled(database: Database): void {
  invariant(
    database._actionsEnabled,
    '[Sync] To use Sync, Actions must be enabled. Pass `{ actionsEnabled: true }` to Database constructor — see docs for more details',
  )
}

export function isSameDatabase(database: Database, initialResetCount: number) {
  return database._resetCount === initialResetCount
}

export function ensureSameDatabase(database: Database, initialResetCount: number): void {
  invariant(
    database._resetCount === initialResetCount,
    `[Sync] Sync aborted because database was reset`,
  )
}

export const isChangeSetEmpty: (arg1: SyncDatabaseChangeSet) => boolean = pipe(
  values,
  all(
    // @ts-ignore
    ({ created, updated, deleted }) => created.length + updated.length + deleted.length === 0,
  ),
)
