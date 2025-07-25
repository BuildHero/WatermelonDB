import clone from 'lodash.clonedeep'
import { change, times, map, length } from 'rambdax'
import { skip as skip$ } from 'rxjs/operators'
import { noop } from '../utils/fp'
import { randomId } from '../utils/common'
import { mockDatabase, testSchema } from '../__tests__/testModels'
import { expectToRejectWithMessage } from '../__tests__/utils'
import { sanitizedRaw } from '../RawRecord'
import { schemaMigrations, createTable, addColumns } from '../Schema/migrations'

import { synchronize, hasUnsyncedChanges } from './index'
import {
  fetchLocalChanges,
  markLocalChangesAsSynced,
  applyRemoteChanges,
  getLastPulledAt,
  getLastPulledSchemaVersion,
  setLastPulledAt,
  setLastPulledSchemaVersion,
} from './impl'
import { resolveConflict, isChangeSetEmpty } from './impl/helpers'

describe('Conflict resolution', () => {
  it('can resolve per-column conflicts', () => {
    expect(
      resolveConflict(
        { col1: 'a', col2: true, col3: 10, _status: 'updated', _changed: 'col2' },
        { col1: 'b', col2: false, col3: 10 },
      ),
    ).toEqual({ _status: 'updated', _changed: 'col2', col1: 'b', col2: true, col3: 10 })
    expect(
      resolveConflict(
        { col1: 'a', col2: true, col3: 20, _status: 'updated', _changed: 'col2,col3' },
        { col1: 'b', col2: false, col3: 10 },
      ),
    ).toEqual({ _status: 'updated', _changed: 'col2,col3', col1: 'b', col2: true, col3: 20 })
  })
  it('ignores missing remote columns', () => {
    expect(
      resolveConflict(
        { col1: 'a', col2: true, col3: 20, _status: 'updated', _changed: 'col2' },
        { col2: false },
      ),
    ).toEqual({ _status: 'updated', _changed: 'col2', col1: 'a', col2: true, col3: 20 })
  })
})

const makeDatabase = () => mockDatabase({ actionsEnabled: true })

const prepareCreateFromRaw = (collection, dirtyRaw) =>
  collection.prepareCreate(record => {
    record._raw = sanitizedRaw({ _status: 'synced', ...dirtyRaw }, record.collection.schema)
  })

const getRaw = (collection, id) => collection.find(id).then(record => record._raw, () => null)

const expectSyncedAndMatches = async (collection, id, match) =>
  expect(await getRaw(collection, id)).toMatchObject({
    _status: expect.any(String),
    _changed: '',
    id,
    ...match,
  })
const expectDoesNotExist = async (collection, id) => expect(await getRaw(collection, id)).toBe(null)

const emptyChangeSet = {
  mock_projects: { created: [], updated: [], deleted: [] },
  mock_tasks: { created: [], updated: [], deleted: [] },
  mock_comments: { created: [], updated: [], deleted: [] },
}

const emptyLocalChanges = Object.freeze({ changes: emptyChangeSet, affectedRecords: [] })

const makeChangeSet = set => {
  // Convert Map to object for the change function, then back to Map
  const emptyChangeSetObj = {}
  const result = change(emptyChangeSetObj, '', set)
  // Convert object to Map since applyRemoteChanges expects a Map
  return new Map(Object.entries(result))
}

const testApplyRemoteChanges = (db, set) => applyRemoteChanges(db, makeChangeSet(set))

const sorted = models => {
  const copy = models.slice()
  copy.sort((a, b) => {
    if (a.id < b.id) {
      return -1
    } else if (a.id > b.id) {
      return 1
    }
    return 0
  })
  return copy
}

const makeLocalChanges = database =>
  database.action(async () => {
    const projects = database.get('mock_projects')
    const tasks = database.get('mock_tasks')
    const comments = database.get('mock_comments')

    // create records
    const created = obj => ({ _status: 'created', ...obj })
    const timestamps = { created_at: 1000, updated_at: 2000 }

    const records = {
      pSynced: prepareCreateFromRaw(projects, { id: 'pSynced' }),
      pCreated1: prepareCreateFromRaw(projects, created({ id: 'pCreated1' })),
      pCreated2: prepareCreateFromRaw(projects, created({ id: 'pCreated2' })),
      pUpdated: prepareCreateFromRaw(projects, { id: 'pUpdated' }),
      pDeleted: prepareCreateFromRaw(projects, { id: 'pDeleted' }),
      tSynced: prepareCreateFromRaw(tasks, { id: 'tSynced' }),
      tCreated: prepareCreateFromRaw(tasks, created({ id: 'tCreated' })),
      tUpdated: prepareCreateFromRaw(tasks, {
        id: 'tUpdated',
        name: 'orig',
        description: 'orig',
        project_id: 'orig',
      }),
      tDeleted: prepareCreateFromRaw(tasks, { id: 'tDeleted' }),
      cSynced: prepareCreateFromRaw(comments, { id: 'cSynced', ...timestamps }),
      cCreated: prepareCreateFromRaw(comments, created({ id: 'cCreated', ...timestamps })),
      cUpdated: prepareCreateFromRaw(comments, { id: 'cUpdated', ...timestamps }),
      cDeleted: prepareCreateFromRaw(comments, { id: 'cDeleted', ...timestamps }),
      cDestroyed: prepareCreateFromRaw(comments, { id: 'cDestroyed' }),
    }

    await database.batch(...Object.values(records))

    // update records
    await records.pUpdated.update(p => {
      p.name = 'local'
    })
    await records.tUpdated.update(p => {
      p.name = 'local'
      p.position = 100
    })
    await records.cUpdated.update(c => {
      c.body = 'local'
    })
    await records.tDeleted.update(t => {
      t.name = 'local'
    })

    // delete records
    await records.pDeleted.markAsDeleted()
    await records.tDeleted.markAsDeleted()
    await records.cDeleted.markAsDeleted()
    await records.cDestroyed.destroyPermanently() // sanity check

    return records
  })

describe('fetchLocalChanges', () => {
  it('returns empty object if no changes', async () => {
    const { database } = makeDatabase()
    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)
  })
  it('fetches all local changes', async () => {
    // eslint-disable-next-line
    let { database, cloneDatabase } = makeDatabase()

    const {
      pCreated1,
      pCreated2,
      pUpdated,
      tCreated,
      tUpdated,
      tDeleted,
      cCreated,
      cUpdated,
    } = await makeLocalChanges(database)

    // check
    expect(pCreated1._raw._status).toBe('created')
    expect(pUpdated._raw._status).toBe('updated')
    expect(pUpdated._raw._changed).toBe('name')

    expect(tDeleted._raw._status).toBe('deleted')
    const expectedChanges = clone({
      mock_projects: {
        created: [pCreated2._raw, pCreated1._raw],
        updated: [pUpdated._raw],
        deleted: ['pDeleted'],
      },
      mock_tasks: { created: [tCreated._raw], updated: [tUpdated._raw], deleted: ['tDeleted'] },
      mock_comments: {
        created: [cCreated._raw],
        updated: [cUpdated._raw],
        deleted: ['cDeleted'],
      },
    })
    const expectedAffectedRecords = [
      pCreated2,
      pCreated1,
      pUpdated,
      tCreated,
      tUpdated,
      cCreated,
      cUpdated,
    ]
    const result = await fetchLocalChanges(database)
    expect(result.changes).toEqual(expectedChanges)
    expect(result.affectedRecords).toEqual(expectedAffectedRecords)

    // simulate reload
    database = await cloneDatabase()
    const result2 = await fetchLocalChanges(database)
    expect(result2.changes).toEqual(expectedChanges)
    expect(result2.affectedRecords.map(r => r._raw)).toEqual(
      expectedAffectedRecords.map(r => r._raw),
    )
  })
  it('returns object copies', async () => {
    const { database } = makeDatabase()

    const { pUpdated } = await makeLocalChanges(database)

    const { changes } = await fetchLocalChanges(database)
    const changesCloned = clone(changes)

    // raws should be cloned - further changes don't affect result
    await database.action(() =>
      pUpdated.update(p => {
        p.name = 'y'
      }),
    )
    expect(changes).toEqual(changesCloned)
  })
})

describe('hasUnsyncedChanges', () => {
  it('has no unsynced changes by default', async () => {
    const { database } = makeDatabase()
    expect(await hasUnsyncedChanges({ database })).toBe(false)
  })
  it('has unsynced changes if made', async () => {
    const { database } = makeDatabase()
    await makeLocalChanges(database)
    expect(await hasUnsyncedChanges({ database })).toBe(true)
  })
  it('just one update is enough', async () => {
    const { database } = makeDatabase()
    const collection = database.get('mock_comments')
    const record = await database.action(() =>
      collection.create(rec => {
        rec._raw._status = 'synced'
      }),
    )

    expect(await hasUnsyncedChanges({ database })).toBe(false)

    await database.action(async () => {
      await record.update(() => {
        record.body = 'changed'
      })
    })

    expect(await hasUnsyncedChanges({ database })).toBe(true)
  })
  it('just one delete is enough', async () => {
    const { database } = makeDatabase()
    const collection = database.get('mock_comments')
    const record = await database.action(() =>
      collection.create(rec => {
        rec._raw._status = 'synced'
      }),
    )

    expect(await hasUnsyncedChanges({ database })).toBe(false)

    await database.action(() => record.markAsDeleted())

    expect(await hasUnsyncedChanges({ database })).toBe(true)
  })
  it('aborts if actions are not enabled', async () => {
    const { database } = mockDatabase({ actionsEnabled: false })

    await expectToRejectWithMessage(hasUnsyncedChanges({ database }), /actions must be enabled/i)
  })
})

describe('isChangeSetEmpty', () => {
  it('empty changeset is empty', () => {
    expect(isChangeSetEmpty(emptyChangeSet)).toBe(true)
    expect(isChangeSetEmpty(new Map())).toBe(true)
  })
})

describe('markLocalChangesAsSynced', () => {
  it('does nothing for empty local changes', async () => {
    const { database } = makeDatabase()

    await makeLocalChanges(database)
    const localChanges1 = await fetchLocalChanges(database)

    await markLocalChangesAsSynced(database, { changes: emptyChangeSet, affectedRecords: [] })

    const localChanges2 = await fetchLocalChanges(database)
    expect(localChanges1).toEqual(localChanges2)
  })
  it('marks local changes as synced', async () => {
    const { database, projects, tasks } = makeDatabase()

    await makeLocalChanges(database)

    const projectCount = await projects.query().fetchCount()
    const taskCount = await tasks.query().fetchCount()

    await markLocalChangesAsSynced(database, await fetchLocalChanges(database))

    // no more changes
    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)

    // still just as many objects
    const projectList = await projects.query().fetch()
    const taskList = await tasks.query().fetch()
    expect(projectList.length).toBe(projectCount)
    expect(taskList.length).toBe(taskCount)

    // all objects marked as synced
    expect(projectList.every(record => record.syncStatus === 'synced')).toBe(true)
    expect(taskList.every(record => record.syncStatus === 'synced')).toBe(true)

    // no objects marked as deleted
    expect(await database.adapter.getDeletedRecords('mock_projects')).toEqual([])
    expect(await database.adapter.getDeletedRecords('mock_tasks')).toEqual([])
    expect(await database.adapter.getDeletedRecords('mock_comments')).toEqual([])
  })
  it(`doesn't modify updated_at timestamps`, async () => {
    const { database, comments } = makeDatabase()

    await makeLocalChanges(database)
    const updatedAt = (await getRaw(comments, 'cUpdated')).updated_at
    await markLocalChangesAsSynced(database, await fetchLocalChanges(database))

    await expectSyncedAndMatches(comments, 'cCreated', { created_at: 1000, updated_at: 2000 })
    await expectSyncedAndMatches(comments, 'cUpdated', { created_at: 1000, updated_at: updatedAt })
    await expectSyncedAndMatches(comments, 'cSynced', { created_at: 1000, updated_at: 2000 })
  })
  it(`doesn't mark as synced records that changed since changes were fetched`, async () => {
    const { database, projects, tasks } = makeDatabase()

    const {
      pSynced,
      tSynced,
      tCreated,
      tUpdated,
      cSynced,
      cCreated,
      cUpdated,
      cDeleted,
    } = await makeLocalChanges(database)
    const localChanges = await fetchLocalChanges(database)

    // simulate user making changes the the app while sync push request is in progress
    let newProject
    await database.action(async () => {
      // non-confliting changes: new record, update synced record, delete synced record
      newProject = await projects.create()
      await pSynced.update(() => {
        pSynced.name = 'local2'
      })
      await tSynced.markAsDeleted()
      await cSynced.destroyPermanently()

      // conflicting changes: update updated/created, delete created/updated/deleted
      await tCreated.update(() => {
        tCreated.name = 'local2'
      })
      await tUpdated.update(() => {
        tUpdated.name = 'local2' // change what was already changed
        tUpdated.description = 'local2' // new change
      })
      await cCreated.markAsDeleted()
      await cUpdated.markAsDeleted()
      await cDeleted.destroyPermanently()
    })

    // mark local changes as synced; check if new changes are still pending sync
    await markLocalChangesAsSynced(database, localChanges)

    const localChanges2 = await fetchLocalChanges(database)
    expect(localChanges2.changes).toEqual({
      mock_projects: { created: [newProject._raw], updated: [pSynced._raw], deleted: [] },
      mock_tasks: { created: [tCreated._raw], updated: [tUpdated._raw], deleted: ['tSynced'] },
      mock_comments: { created: [], updated: [], deleted: ['cUpdated', 'cCreated'] },
    })
    expect(sorted(localChanges2.affectedRecords)).toEqual(
      sorted([newProject, tCreated, pSynced, tUpdated]),
    )

    await expectSyncedAndMatches(tasks, 'tUpdated', {
      _status: 'updated',
      // TODO: ideally position would probably not be here
      _changed: 'name,position,description',
      name: 'local2',
      description: 'local2',
      position: 100,
    })

    // test that second push will mark all as synced
    await markLocalChangesAsSynced(database, localChanges2)
    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)
  })
  it('only emits one collection batch change', async () => {
    const { database, projects } = makeDatabase()

    const { pCreated1 } = await makeLocalChanges(database)
    const localChanges = await fetchLocalChanges(database)

    const projectsObserver = jest.fn()
    projects.changes.subscribe(projectsObserver)

    await markLocalChangesAsSynced(database, localChanges)

    expect(projectsObserver).toHaveBeenCalledTimes(1)
  })
  it(`doesn't send _status, _changed fields`, async () => {
    // TODO: Future improvement
  })
  it('only returns changed fields', async () => {
    // TODO: Possible future improvement?
  })
})

describe('applyRemoteChanges', () => {
  it('does nothing if no remote changes', async () => {
    const { database } = makeDatabase()

    await makeLocalChanges(database)
    const localChanges1 = await fetchLocalChanges(database)

    await applyRemoteChanges(database, new Map(Object.entries(emptyChangeSet)))

    const localChanges2 = await fetchLocalChanges(database)
    expect(localChanges1).toEqual(localChanges2)
  })
  // Note: We need to test all possible status combinations - xproduct of:
  // remote: created/updated/deleted
  // local: synced/created/updated/deleted/doesn't exist
  // (15 cases)
  it('can create, update, delete records', async () => {
    const { database, projects, tasks, comments } = makeDatabase()

    await makeLocalChanges(database)
    await testApplyRemoteChanges(database, {
      mock_projects: {
        // create / doesn't exist - create
        created: [{ id: 'new_project', name: 'remote' }],
      },
      mock_tasks: {
        // update / synced - update (stay synced)
        updated: [{ id: 'tSynced', name: 'remote' }],
      },
      mock_comments: {
        // delete / synced - destroy
        deleted: ['cSynced'],
      },
    })

    await expectSyncedAndMatches(projects, 'new_project', { name: 'remote' })
    await expectSyncedAndMatches(tasks, 'tSynced', { name: 'remote' })
    await expectDoesNotExist(comments, 'cSynced')
  })
  it('can resolve update conflicts', async () => {
    const { database, tasks, comments } = makeDatabase()

    await makeLocalChanges(database)
    await testApplyRemoteChanges(database, {
      mock_tasks: {
        updated: [
          // update / updated - resolve and update (stay updated)
          { id: 'tUpdated', name: 'remote', description: 'remote' },
        ],
      },
      mock_comments: {
        // update / deleted - ignore (will be synced anyway)
        updated: [{ id: 'cDeleted', body: 'remote' }],
      },
    })

    await expectSyncedAndMatches(tasks, 'tUpdated', {
      _status: 'updated',
      _changed: 'name,position',
      name: 'local', // local change preserved
      position: 100,
      description: 'remote', // remote change
      project_id: 'orig', // unchanged
    })
    await expectSyncedAndMatches(comments, 'cDeleted', { _status: 'deleted', body: '' })
  })

  it('can handle sync failure cases', async () => {
    const { database, tasks } = makeDatabase()

    await makeLocalChanges(database)
    await testApplyRemoteChanges(database, {
      mock_tasks: {
        // these cases can occur when sync fails for some reason and the same records are fetched and reapplied:
        created: [
          // create / synced - resolve and update (stay synced)
          { id: 'tSynced', name: 'remote' },
          // create / updated - resolve and update (stay updated)
          { id: 'tUpdated', name: 'remote', description: 'remote' },
          // create / deleted - destroy and recreate? (or just un-delete?)
          { id: 'tDeleted', name: 'remote' },
        ],
      },
    })

    await expectSyncedAndMatches(tasks, 'tSynced', { name: 'remote' })
    await expectSyncedAndMatches(tasks, 'tUpdated', {
      _status: 'updated',
      _changed: 'name,position',
      name: 'local', // local change preserved
      position: 100,
      description: 'remote', // remote change
      project_id: 'orig', // unchanged
    })
    await expectSyncedAndMatches(tasks, 'tDeleted', { name: 'remote' })
  })
  it('can handle weird edge cases', async () => {
    const { database, projects, tasks } = makeDatabase()

    await makeLocalChanges(database)
    await testApplyRemoteChanges(database, {
      mock_projects: {
        created: [
          // create / created - very weird case. update with resolution (stay synced)
          { id: 'pCreated', name: 'remote' },
        ],
      },
      mock_tasks: {
        updated: [
          // update / created - very weird. resolve and update (stay synced)
          { id: 'tCreated', name: 'remote' },
          // update / doesn't exist - create (stay synced)
          { id: 'does_not_exist', name: 'remote' },
        ],
      },
    })

    await expectSyncedAndMatches(projects, 'pCreated', { name: 'remote' })
    await expectSyncedAndMatches(tasks, 'tCreated', { name: 'remote' })
    await expectSyncedAndMatches(tasks, 'does_not_exist', { name: 'remote' })
  })
  it(`doesn't touch created_at/updated_at when applying updates`, async () => {
    const { database, comments } = makeDatabase()

    await makeLocalChanges(database)
    await testApplyRemoteChanges(database, {
      mock_comments: {
        updated: [{ id: 'cSynced', body: 'remote' }],
      },
    })

    await expectSyncedAndMatches(comments, 'cSynced', {
      created_at: 1000,
      updated_at: 2000,
      body: 'remote',
    })
  })
  it('can replace created_at/updated_at during sync', async () => {
    const { database, comments } = makeDatabase()

    await makeLocalChanges(database)
    await testApplyRemoteChanges(database, {
      mock_comments: {
        created: [{ id: 'cNew', created_at: 1, updated_at: 2 }],
        updated: [{ id: 'cSynced', created_at: 10, updated_at: 20 }],
      },
    })

    await expectSyncedAndMatches(comments, 'cNew', { created_at: 1, updated_at: 2, body: '' })
    await expectSyncedAndMatches(comments, 'cSynced', { created_at: 10, updated_at: 20, body: '' })
  })
  it(`doesn't destroy dependent objects`, async () => {
    // TODO: Add this test when fast delete is implemented
  })
  it('only emits one collection batch change', async () => {
    // TODO: Implement and unskip test when batch change emissions are implemented
  })
  it('rejects invalid records', async () => {
    const { database } = makeDatabase()

    const expectChangeFails = changes =>
      expectToRejectWithMessage(
        testApplyRemoteChanges(database, { mock_projects: changes }),
        /invalid raw record/i,
      )

    const expectCreateFails = raw => expectChangeFails({ created: [raw] })
    const expectUpdateFails = raw => expectChangeFails({ updated: [raw] })

    await expectCreateFails({ id: 'foo', _status: 'created' })
    await expectCreateFails({ id: 'foo', _changed: 'bla' })
    await expectCreateFails({ foo: 'bar' })

    await expectUpdateFails({ id: 'foo', _status: 'created' })
    await expectUpdateFails({ id: 'foo', _changed: 'bla' })
    await expectUpdateFails({ foo: 'bar' })

    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)
  })
  it(`safely skips collections that don't exist`, async () => {
    const { database } = makeDatabase()

    await testApplyRemoteChanges(database, { invalid_project: { created: [{ id: 'foo' }] } })
    await testApplyRemoteChanges(database, { __proto__: { created: [{ id: 'foo' }] } }) // oof, naughty
  })
})

const observeDatabase = database => {
  const observer = jest.fn()
  const tables = ['mock_projects', 'mock_tasks', 'mock_comments']
  expect(tables).toEqual(Object.keys(database.collections.map))
  database
    .withChangesForTables(tables)
    .pipe(skip$(1))
    .subscribe(observer)
  return observer
}

const emptyPull = (timestamp = 1500) => async () => ({
  changes: new Map(Object.entries(emptyChangeSet)),
  timestamp,
})

describe('synchronize', () => {
  it('can perform an empty sync', async () => {
    const { database } = makeDatabase()
    const observer = observeDatabase(database)

    const pullChanges = jest.fn(emptyPull())

    await synchronize({ database, pullChanges, pushChanges: jest.fn() })

    expect(observer).toHaveBeenCalledTimes(0)
    expect(pullChanges).toHaveBeenCalledTimes(1)
    expect(pullChanges).toHaveBeenCalledWith({
      lastPulledAt: null,
      schemaVersion: 1,
      migration: null,
    })
  })
  it(`doesn't push changes if nothing to push`, async () => {
    const { database } = makeDatabase()

    const pushChanges = jest.fn()
    await synchronize({ database, pullChanges: jest.fn(emptyPull()), pushChanges })

    expect(pushChanges).toHaveBeenCalledTimes(0)
  })
  it('can log basic information about a sync', async () => {
    const { database } = makeDatabase()

    const log = {}
    await synchronize({
      database,
      pullChanges: async () => {
        // ensure we take more than 1ms for the log test
        await new Promise(resolve => setTimeout(resolve, 10))
        return emptyPull()()
      },
      pushChanges: () => {},
      log,
    })

    expect(log.startedAt).toBeInstanceOf(Date)
    expect(log.finishedAt).toBeInstanceOf(Date)
    expect(log.finishedAt.getTime()).toBeGreaterThan(log.startedAt.getTime())

    expect(log.lastPulledAt).toBe(null)
    expect(log.newLastPulledAt).toBe(1500)
  })
  it('can push changes', async () => {
    const { database } = makeDatabase()

    await makeLocalChanges(database)
    const localChanges = await fetchLocalChanges(database)

    const pullChanges = jest.fn(emptyPull())
    const pushChanges = jest.fn()
    await synchronize({ database, pullChanges, pushChanges })

    expect(pushChanges).toHaveBeenCalledWith({ changes: localChanges.changes, lastPulledAt: 1500 })
    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)
  })
  it('can pull changes', async () => {
    const { database, projects, tasks } = makeDatabase()

    const pullChanges = jest.fn(async () => ({
      changes: makeChangeSet({
        mock_projects: {
          created: [{ id: 'new_project', name: 'remote' }],
          updated: [{ id: 'pSynced', name: 'remote' }],
        },
        mock_tasks: {
          deleted: ['tSynced'],
        },
      }),
      timestamp: 1500,
    }))

    await synchronize({ database, pullChanges, pushChanges: jest.fn() })

    expect(pullChanges).toHaveBeenCalledWith({
      lastPulledAt: null,
      schemaVersion: 1,
      migration: null,
    })

    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)
    await expectSyncedAndMatches(projects, 'new_project', { name: 'remote' })
    await expectSyncedAndMatches(projects, 'pSynced', { name: 'remote' })
    await expectDoesNotExist(tasks, 'tSynced')
  })
  it('can synchronize changes with conflicts', async () => {
    const { database, projects, tasks, comments } = makeDatabase()

    const records = await makeLocalChanges(database)
    const tUpdatedInitial = { ...records.tUpdated._raw }
    const cUpdatedInitial = { ...records.cUpdated._raw }

    const localChanges = await fetchLocalChanges(database)

    const pullChanges = async () => ({
      changes: makeChangeSet({
        mock_projects: {
          created: [{ id: 'pCreated1', name: 'remote' }], // error - update, stay synced
          deleted: ['pUpdated', 'does_not_exist', 'pDeleted'],
        },
        mock_tasks: {
          updated: [
            { id: 'tUpdated', name: 'remote', description: 'remote' }, // just a conflict; stay updated
            { id: 'tDeleted', body: 'remote' }, // ignore
          ],
        },
        mock_comments: {
          created: [
            { id: 'cUpdated', body: 'remote', task_id: 'remote' }, // error - resolve and update (stay updated)
          ],
        },
      }),
      timestamp: 1500,
    })
    const pushChanges = jest.fn()

    const log = {}
    await synchronize({ database, pullChanges, pushChanges, log })

    expect(pushChanges).toHaveBeenCalledTimes(1)
    const pushedChanges = pushChanges.mock.calls[0][0].changes
    expect(pushedChanges).not.toEqual(localChanges.changes)
    expect(pushedChanges.mock_projects.created).not.toContainEqual(
      await getRaw(projects, 'pCreated1'),
    )
    expect(pushedChanges.mock_projects.deleted).not.toContain(['pDeleted'])
    const tUpdatedResolvedExpected = {
      // TODO: That's just dirty
      ...(await getRaw(tasks, 'tUpdated')),
      _status: 'updated',
      _changed: 'name,position',
    }
    expect(pushedChanges.mock_tasks.updated).toContainEqual(tUpdatedResolvedExpected)
    expect(pushedChanges.mock_tasks.deleted).toContain('tDeleted')
    const cUpdatedResolvedExpected = {
      // TODO: That's just dirty
      ...(await getRaw(comments, 'cUpdated')),
      _status: 'updated',
      _changed: 'updated_at,body',
    }
    expect(pushedChanges.mock_comments.updated).toContainEqual(cUpdatedResolvedExpected)

    await expectSyncedAndMatches(projects, 'pCreated1', { name: 'remote' })
    await expectDoesNotExist(projects, 'pUpdated')
    await expectDoesNotExist(projects, 'pDeleted')
    await expectSyncedAndMatches(tasks, 'tUpdated', { name: 'local', description: 'remote' })
    await expectDoesNotExist(tasks, 'tDeleted')
    await expectSyncedAndMatches(comments, 'cUpdated', { body: 'local', task_id: 'remote' })

    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)

    // check that log is good
    expect(log.resolvedConflicts).toEqual([
      {
        local: tUpdatedInitial,
        remote: { id: 'tUpdated', name: 'remote', description: 'remote' },
        resolved: tUpdatedResolvedExpected,
      },
      {
        local: cUpdatedInitial,
        remote: { id: 'cUpdated', body: 'remote', task_id: 'remote' },
        resolved: cUpdatedResolvedExpected,
      },
    ])
  })
  it(`allows conflict resolution to be customized`, async () => {
    const { database, projects, tasks } = makeDatabase()

    await database.action(async () => {
      await database.batch(
        prepareCreateFromRaw(projects, { id: 'p1', _status: 'synced', name: 'local' }),
        prepareCreateFromRaw(projects, { id: 'p2', _status: 'created', name: 'local' }),
        prepareCreateFromRaw(tasks, { id: 't1', _status: 'synced' }),
        prepareCreateFromRaw(tasks, { id: 't2', _status: 'created' }),
        prepareCreateFromRaw(tasks, {
          id: 't3',
          _status: 'updated',
          name: 'local',
          _changd: 'name',
        }),
      )
    })

    const conflictResolver = jest.fn((table, local, remote, resolved) => {
      if (table === 'mock_tasks') {
        resolved.name = 'GOTCHA'
      }
      return resolved
    })

    const pullChanges = async () => ({
      changes: makeChangeSet({
        mock_projects: {
          created: [{ id: 'p2', name: 'remote' }], // error - update, stay synced
          updated: [{ id: 'p1', name: 'change' }], // update
        },
        mock_tasks: {
          updated: [
            { id: 't1', name: 'remote' }, // update
            { id: 't3', name: 'remote' }, // conflict
          ],
        },
      }),
      timestamp: 1500,
    })
    await synchronize({ database, pullChanges, pushChanges: jest.fn(), conflictResolver })

    expect(conflictResolver).toHaveBeenCalledTimes(4)
    expect(conflictResolver.mock.calls[0]).toMatchObject([
      'mock_projects',
      { id: 'p2', _status: 'created', name: 'local' },
      { name: 'remote' },
      { name: 'remote' },
    ])
    expect(conflictResolver.mock.calls[1]).toMatchObject([
      'mock_projects',
      { id: 'p1', _status: 'synced' },
      { name: 'change' },
      { _status: 'synced' },
    ])
    expect(conflictResolver.mock.results[1].value).toBe(conflictResolver.mock.calls[1][3])
    expect(conflictResolver.mock.calls[2]).toMatchObject([
      'mock_tasks',
      { id: 't1', _status: 'synced', name: '' },
      { name: 'remote' },
      { name: 'GOTCHA' }, // we're mutating this arg in function, that's why
    ])

    await expectSyncedAndMatches(tasks, 't1', { name: 'GOTCHA' })
    await expectSyncedAndMatches(tasks, 't3', { name: 'GOTCHA' })

    expect(await fetchLocalChanges(database)).toEqual(emptyLocalChanges)
  })
  it('remembers last_synced_at timestamp', async () => {
    const { database } = makeDatabase()

    let pullChanges = jest.fn(emptyPull(1500))
    await synchronize({ database, pullChanges, pushChanges: jest.fn() })

    expect(pullChanges).toHaveBeenCalledWith({
      lastPulledAt: null,
      schemaVersion: 1,
      migration: null,
    })

    pullChanges = jest.fn(emptyPull(2500))
    const log = {}
    await synchronize({ database, pullChanges, pushChanges: jest.fn(), log })

    expect(pullChanges).toHaveBeenCalledTimes(1)
    expect(pullChanges).toHaveBeenCalledWith({
      lastPulledAt: 1500,
      schemaVersion: 1,
      migration: null,
    })
    expect(await getLastPulledAt(database)).toBe(2500)
    expect(log.lastPulledAt).toBe(1500)
    expect(log.newLastPulledAt).toBe(2500)
    // check underlying database since it's an implicit API
    expect(await database.adapter.getLocal('__watermelon_last_pulled_at')).toBe('2500')
  })

  it('remember last_sync_at timestamp and sequence_id', async () => {
    const { database } = makeDatabase()

    let pullChanges = jest.fn(emptyPull(1500))

    // intentionally not setting useSequenceIds to true
    // to test backward compat with regular timestamps
    await synchronize({ database, pullChanges, pushChanges: jest.fn() })

    expect(pullChanges).toHaveBeenCalledWith({
      lastPulledAt: null,
      schemaVersion: 1,
      migration: null,
    })

    pullChanges = jest.fn(emptyPull('000002C9D02JT1PBBTR4TPSRFX'))
    const log = {}
    await synchronize({ database, pullChanges, pushChanges: jest.fn(), log, useSequenceIds: true })

    expect(pullChanges).toHaveBeenCalledTimes(1)
    expect(pullChanges).toHaveBeenCalledWith(
      expect.objectContaining({
        lastPulledAt: expect.stringContaining('000001DRV'),
        schemaVersion: 1,
        migration: null,
      }),
    )

    expect(await getLastPulledAt(database)).toBe(2500)
    expect(log.lastPulledAt).toContain('000001DRV')
    expect(log.newLastPulledAt).toBe('000002C9D02JT1PBBTR4TPSRFX')
    // check underlying database since it's an implicit API
    expect(await database.adapter.getLocal('__watermelon_last_pulled_at')).toBe('2500')
    expect(await database.adapter.getLocal('__watermelon_last_sequence_id')).toBe(
      '000002C9D02JT1PBBTR4TPSRFX',
    )
  })

  it('can recover from pull failure', async () => {
    const { database } = makeDatabase()
    // make change to make sure pushChagnes isn't called because of pull failure and not lack of changes
    await makeLocalChanges(database)

    const observer = observeDatabase(database)
    const pullChanges = jest.fn(() => Promise.reject(new Error('pull-fail')))
    const pushChanges = jest.fn()
    const sync = await synchronize({ database, pullChanges, pushChanges }).catch(e => e)

    expect(observer).toHaveBeenCalledTimes(0)
    expect(pullChanges).toHaveBeenCalledTimes(1)
    expect(pushChanges).toHaveBeenCalledTimes(0)
    expect(sync).toMatchObject({ message: 'pull-fail' })
    expect(await getLastPulledAt(database)).toBe(null)
  })
  it('can recover from push failure', async () => {
    const { database, projects } = makeDatabase()

    await makeLocalChanges(database)
    const localChanges = await fetchLocalChanges(database)

    const observer = observeDatabase(database)
    const pullChanges = async () => ({
      changes: makeChangeSet({
        mock_projects: {
          created: [{ id: 'new_project', name: 'remote' }],
        },
      }),
      timestamp: 1500,
    })
    const pushChanges = jest.fn(() => Promise.reject(new Error('push-fail')))
    const sync = await synchronize({ database, pullChanges, pushChanges }).catch(e => e)

    // full sync failed - local changes still awaiting sync
    expect(pushChanges).toHaveBeenCalledWith({ changes: localChanges.changes, lastPulledAt: 1500 })
    expect(sync).toMatchObject({ message: 'push-fail' })
    expect(await fetchLocalChanges(database)).toEqual(localChanges)

    // but pull phase succeeded
    expect(await getLastPulledAt(database)).toBe(1500)
    expect(observer).toHaveBeenCalledTimes(1)
    await expectSyncedAndMatches(projects, 'new_project', { name: 'remote' })
  })

  it('aborts if actions are not enabled', async () => {
    const { database } = mockDatabase({ actionsEnabled: false })

    await expectToRejectWithMessage(
      synchronize({ database, pullChanges: jest.fn(), pushChanges: jest.fn() }),
      /actions must be enabled/i,
    )
  })
  describe('migration syncs', () => {
    const testSchema10 = { ...testSchema, version: 10 }
    const migrations = schemaMigrations({
      migrations: [
        {
          toVersion: 10,
          steps: [
            addColumns({
              table: 'attachment_versions',
              columns: [{ name: 'reactions', type: 'string' }],
            }),
          ],
        },
        {
          toVersion: 9,
          steps: [
            createTable({
              name: 'attachments',
              columns: [{ name: 'parent_id', type: 'string', isIndexed: true }],
            }),
          ],
        },
        { toVersion: 8, steps: [] },
      ],
    })
    it(`remembers synced schema version on first sync`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })
      const pullChanges = jest.fn(emptyPull())

      await synchronize({
        database,
        pullChanges,
        pushChanges: jest.fn(),
        migrationsEnabledAtVersion: 7,
      })
      expect(pullChanges).toHaveBeenCalledWith({
        lastPulledAt: null,
        schemaVersion: 10,
        migration: null,
      })
      expect(await getLastPulledSchemaVersion(database)).toBe(10)
      // check underlying database since it's an implicit API
      expect(await database.adapter.getLocal('__watermelon_last_pulled_schema_version')).toBe('10')
    })
    it(`remembers synced schema version on first sync, even if migrations are not enabled`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10 })
      const pullChanges = jest.fn(emptyPull())

      await synchronize({ database, pullChanges, pushChanges: jest.fn() })
      expect(pullChanges).toHaveBeenCalledWith({
        lastPulledAt: null,
        schemaVersion: 10,
        migration: null,
      })
      expect(await getLastPulledSchemaVersion(database)).toBe(10)
    })
    it(`does not remember schema version if migration syncs are not enabled`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10 })
      await setLastPulledAt(database, 100)
      const pullChanges = jest.fn(emptyPull())

      await synchronize({ database, pullChanges, pushChanges: jest.fn() })
      expect(pullChanges).toHaveBeenCalledWith({
        lastPulledAt: 100,
        schemaVersion: 10,
        migration: null,
      })
      expect(await getLastPulledSchemaVersion(database)).toBe(null)
    })
    it(`performs no migration if up to date`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })
      await setLastPulledAt(database, 1500)
      await setLastPulledSchemaVersion(database, 10)

      const pullChanges = jest.fn(emptyPull(2500))
      await synchronize({
        database,
        pullChanges,
        pushChanges: jest.fn(),
        migrationsEnabledAtVersion: 7,
      })
      expect(pullChanges).toHaveBeenCalledWith({
        lastPulledAt: 1500,
        schemaVersion: 10,
        migration: null,
      })
      expect(await getLastPulledSchemaVersion(database)).toBe(10)
    })
    it(`performs migration sync on schema version bump`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })
      await setLastPulledAt(database, 1500)
      await setLastPulledSchemaVersion(database, 9)

      const pullChanges = jest.fn(emptyPull(2500))
      await synchronize({
        database,
        pullChanges,
        pushChanges: jest.fn(),
        migrationsEnabledAtVersion: 7,
      })
      expect(pullChanges).toHaveBeenCalledWith({
        lastPulledAt: 1500,
        schemaVersion: 10,
        migration: {
          from: 9,
          tables: [],
          columns: [{ table: 'attachment_versions', columns: ['reactions'] }],
        },
      })
      expect(await getLastPulledSchemaVersion(database)).toBe(10)
    })
    it(`performs fallback migration sync`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })
      await setLastPulledAt(database, 1500)

      const pullChanges = jest.fn(emptyPull(2500))
      await synchronize({
        database,
        pullChanges,
        pushChanges: jest.fn(),
        migrationsEnabledAtVersion: 8,
      })
      expect(pullChanges).toHaveBeenCalledWith({
        lastPulledAt: 1500,
        schemaVersion: 10,
        migration: {
          from: 8,
          tables: ['attachments'],
          columns: [{ table: 'attachment_versions', columns: ['reactions'] }],
        },
      })
      expect(await getLastPulledSchemaVersion(database)).toBe(10)
    })
    it(`does not remember schema version if pull fails`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })
      await synchronize({
        database,
        pullChanges: jest.fn(() => Promise.reject(new Error('pull-fail'))),
        pushChanges: jest.fn(),
        migrationsEnabledAtVersion: 8,
      }).catch(e => e)
      expect(await getLastPulledSchemaVersion(database)).toBe(null)
    })
    it(`fails on programmer errors`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })

      await expectToRejectWithMessage(
        synchronize({ database, migrationsEnabledAtVersion: '9' }),
        'Invalid migrationsEnabledAtVersion',
      )
      await expectToRejectWithMessage(
        synchronize({ database, migrationsEnabledAtVersion: 11 }),
        /migrationsEnabledAtVersion must not be greater than current schema version/,
      )
      await expectToRejectWithMessage(
        synchronize({
          database: mockDatabase({ actionsEnabled: true, schema: testSchema10 }).db,
          migrationsEnabledAtVersion: 9,
        }),
        'Migration syncs cannot be enabled on a database that does not support migrations',
      )
      await expectToRejectWithMessage(
        synchronize({ database, migrationsEnabledAtVersion: 6 }),
        `migrationsEnabledAtVersion is too low - not possible to migrate from schema version 6`,
      )
    })
    it(`fails on last synced schema version > current schema version`, async () => {
      const { database } = mockDatabase({ actionsEnabled: true, schema: testSchema10, migrations })
      await setLastPulledAt(database, 1500)
      await setLastPulledSchemaVersion(database, 11)
      await expectToRejectWithMessage(
        synchronize({ database, migrationsEnabledAtVersion: 10 }),
        /Last synced schema version \(11\) is greater than current schema version \(10\)/,
      )
    })
  })
})
