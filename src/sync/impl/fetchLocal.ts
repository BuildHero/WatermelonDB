import {promiseAllObject, map, values, pipe, any, identity} from 'rambdax';
import { unnest, allPromises } from '../../utils/fp'
import type { Database, Collection, Model, TableName } from '../..'
import * as Q from '../../QueryDescription'
import { columnName } from '../../Schema'

import type { SyncTableChangeSet, SyncDatabaseChangeSet } from '../index'
import { ensureActionsEnabled } from './helpers'

export type SyncLocalChanges = {
  changes: SyncDatabaseChangeSet;
  affectedRecords: Model[];
};

// NOTE: Two separate queries are faster than notEq(synced) on LokiJS
const createdQuery = Q.where(columnName('_status'), 'created')
const updatedQuery = Q.where(columnName('_status'), 'updated')

async function fetchLocalChangesForCollection<T extends Model>(collection: Collection<T>): Promise<[SyncTableChangeSet, T[]]> {
  const [createdRecords, updatedRecords, deletedRecords] = await Promise.all([
    collection.query(createdQuery).fetch(),
    collection.query(updatedQuery).fetch(),
    collection.database.adapter.getDeletedRecords(collection.table),
  ])
  const changeSet = {
    created: [],
    updated: [],
    deleted: deletedRecords,
  } as any

  // TODO: It would be best to omit _status, _changed fields, since they're not necessary for the server
  // but this complicates markLocalChangesAsDone, since we don't have the exact copy to compare if record changed
  // TODO: It would probably also be good to only send to server locally changed fields, not full records
  // perf-critical - using mutation
  createdRecords.forEach(record => {
    changeSet.created.push(Object.assign({}, record._raw))
  })
  updatedRecords.forEach(record => {
    changeSet.updated.push(Object.assign({}, record._raw))
  })
  const changedRecords = createdRecords.concat(updatedRecords)

  return [changeSet as any, changedRecords as any]
}

const extractChanges = map(([changeSet]: [any]) => changeSet)
const extractAllAffectedRecords = pipe(
  // @ts-ignore
  values,
  map(([, records]: [any, any]) => records),
  unnest,
)

export default function fetchLocalChanges(db: Database): Promise<SyncLocalChanges> {
  ensureActionsEnabled(db)
  // @ts-ignore
  return db.action(async () => {
    // @ts-ignore
    const changes = await promiseAllObject(map(fetchLocalChangesForCollection, db.collections.map))
    // TODO: deep-freeze changes object (in dev mode only) to detect mutations (user bug)
    return {
      changes: extractChanges(changes as any),
      affectedRecords: extractAllAffectedRecords(changes),
    };
  }, 'sync-fetchLocalChanges');
}

export function hasUnsyncedChanges(db: Database): Promise<boolean> {
  ensureActionsEnabled(db)
  // action is necessary to ensure other code doesn't make changes under our nose
  return db.action(async () => {
    const collections = values(db.collections.map)
    const hasUnsynced = async (collection: Collection<any>) => {
      const created = await collection.query(createdQuery).fetchCount()
      const updated = await collection.query(updatedQuery).fetchCount()
      const deleted = await db.adapter.getDeletedRecords(collection.table)
      return created + updated + deleted.length > 0
    }
    const unsyncedFlags = await allPromises(hasUnsynced, collections as Collection<any>[])
    return any(identity, unsyncedFlags)
  }, 'sync-hasUnsyncedChanges');
}
