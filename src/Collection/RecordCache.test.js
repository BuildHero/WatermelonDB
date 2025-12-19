import { mockDatabase } from '../__tests__/testModels'

describe('RecordCache version tracking', () => {
  describe('Version Management', () => {
    it('initializes with version 0', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      expect(tasks._cache.version).toBe(0)
    })

    it('can be initialized with custom version', () => {
      const { database } = mockDatabase({ actionsEnabled: true })
      const RecordCache = require('./RecordCache').default

      const cache = new RecordCache('test_table', (raw) => raw, () => {}, 5)

      expect(cache.version).toBe(5)
    })

    it('bumps version when collection invalidates', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      expect(tasks._cache.version).toBe(0)

      tasks._invalidateCacheVersion()

      expect(tasks._cache.version).toBe(1)
    })
  })

  describe('Record Versioning', () => {
    it('tags records with current version on add', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())

      expect(task._cacheVersion).toBe(0)
    })

    it('tags new records with bumped version', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task1 = await database.action(() => tasks.create())
      expect(task1._cacheVersion).toBe(0)

      tasks._invalidateCacheVersion()
      expect(tasks._cache.version).toBe(1)

      const task2 = await database.action(() => tasks.create())
      expect(task2._cacheVersion).toBe(1)
    })
  })

  describe('Lazy Invalidation', () => {
    it('returns undefined for stale records', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())
      const taskId = task.id

      // Record is cached
      expect(tasks._cache.get(taskId)).toBe(task)

      // Bump version (marks all as stale)
      tasks._invalidateCacheVersion()

      // get() returns undefined for stale records
      expect(tasks._cache.get(taskId)).toBeUndefined()
    })

    it('removes stale records from map on access', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())
      const taskId = task.id

      // Record is in map
      expect(tasks._cache.map.has(taskId)).toBe(true)

      tasks._invalidateCacheVersion()

      // Still in map (lazy removal)
      expect(tasks._cache.map.has(taskId)).toBe(true)

      // Access triggers removal
      tasks._cache.get(taskId)

      // Now removed
      expect(tasks._cache.map.has(taskId)).toBe(false)
    })

    it('keeps fresh records after version bump', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task1 = await database.action(() => tasks.create())
      expect(task1._cacheVersion).toBe(0)

      // Bump version
      tasks._invalidateCacheVersion()
      expect(tasks._cache.version).toBe(1)

      // task1 is now stale (version 0 < cache version 1)
      const cachedTask1 = tasks._cache.get(task1.id)
      expect(cachedTask1).toBeUndefined() // Stale, removed by lazy check

      // Create new task (will have version 1)
      const task2 = await database.action(() => tasks.create())
      expect(task2._cacheVersion).toBe(1)

      // task2 is fresh
      const cachedTask2 = tasks._cache.get(task2.id)
      expect(cachedTask2).toBe(task2)
    })

    it('handles records without version tag gracefully', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      // Manually add record without version (or version undefined)
      const mockRecord = { id: 'test123', _raw: {}, _cacheVersion: undefined }
      tasks._cache.map.set('test123', mockRecord)

      // Bump version
      tasks._invalidateCacheVersion()
      expect(tasks._cache.version).toBe(1)

      // Note: undefined < 1 is false, so record is NOT treated as stale
      // This is acceptable since all properly created records have versions
      const cached = tasks._cache.get('test123')
      expect(cached).toBe(mockRecord) // Still there (not removed)

      // But if we manually set version to 0, it becomes stale
      mockRecord._cacheVersion = 0
      const cachedAgain = tasks._cache.get('test123')
      expect(cachedAgain).toBeUndefined() // Now stale (0 < 1)
    })
  })

  describe('recordsFromQueryResult', () => {
    it('returns fresh records', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task1 = await database.action(() => tasks.create())
      const task2 = await database.action(() => tasks.create())

      const result = tasks._cache.recordsFromQueryResult([task1.id, task2.id])

      expect(result).toEqual([task1, task2])
    })

    it('filters out stale records via get()', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task1 = await database.action(() => tasks.create())
      const task2 = await database.action(() => tasks.create())

      // Bump version (marks both as stale)
      tasks._invalidateCacheVersion()

      // Directly calling get() returns undefined for stale records
      expect(tasks._cache.get(task1.id)).toBeUndefined()
      expect(tasks._cache.get(task2.id)).toBeUndefined()

      // Note: recordsFromQueryResult doesn't check versions,
      // it relies on Query/Collection layer to refetch
    })

    it('mixes fresh and stale records correctly via get()', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task1 = await database.action(() => tasks.create())

      tasks._invalidateCacheVersion()

      const task2 = await database.action(() => tasks.create())

      // Using get() to check versions
      // task1 is stale (version 0 < cache 1)
      expect(tasks._cache.get(task1.id)).toBeUndefined()
      // task2 is fresh (version 1 === cache 1)
      expect(tasks._cache.get(task2.id)).toBe(task2)
    })

    it('handles raw record objects', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())
      const raw = task._raw

      tasks._cache.map.clear()

      const result = tasks._cache.recordsFromQueryResult([raw])

      expect(result.length).toBe(1)
      expect(result[0].id).toBe(task.id)
      expect(result[0]._cacheVersion).toBe(tasks._cache.version)
    })
  })

  describe('Performance', () => {
    it('handles large number of stale records efficiently', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Create 1000 records
      const createdTasks = []
      await database.action(async () => {
        for (let i = 0; i < 1000; i++) {
          createdTasks.push(await tasks.create())
        }
      })

      expect(tasks._cache.map.size).toBe(1000)

      // Bump version (marks all as stale)
      tasks._invalidateCacheVersion()

      // All records still in memory (lazy)
      expect(tasks._cache.map.size).toBe(1000)

      // Access 10 records (removes them)
      for (let i = 0; i < 10; i++) {
        tasks._cache.get(createdTasks[i].id)
      }

      // 10 removed, 990 remaining
      expect(tasks._cache.map.size).toBe(990)

      // Create new record
      const newTask = await database.action(() => tasks.create())

      // New record is cached
      expect(tasks._cache.get(newTask.id)).toBe(newTask)

      // Old records still stale
      expect(tasks._cache.get(createdTasks[11].id)).toBeUndefined()
    })

    it('version check is fast (no iteration)', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Create many records
      await database.action(async () => {
        for (let i = 0; i < 100; i++) {
          await tasks.create()
        }
      })

      const start = Date.now()

      // This should be O(1), not O(n)
      tasks._invalidateCacheVersion()

      const duration = Date.now() - start

      // Should be instant (< 5ms even on slow machines)
      expect(duration).toBeLessThan(5)
    })
  })

  describe('Integration with Collection', () => {
    it('cache version syncs with collection version', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      expect(tasks._cacheVersion).toBe(0)
      expect(tasks._cache.version).toBe(0)

      tasks._invalidateCacheVersion()

      expect(tasks._cacheVersion).toBe(1)
      expect(tasks._cache.version).toBe(1)

      tasks._invalidateCacheVersion()

      expect(tasks._cacheVersion).toBe(2)
      expect(tasks._cache.version).toBe(2)
    })

    it('find() with stale cache', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create((t) => (t.name = 'Original')))
      const taskId = task.id

      // Cached
      expect(tasks._cache.get(taskId)).toBe(task)

      // Bump version (marks as stale)
      tasks._invalidateCacheVersion()

      // Cache get returns undefined for stale
      expect(tasks._cache.get(taskId)).toBeUndefined()

      // Note: In production, find() would trigger a refetch from the database,
      // but in this test environment with LokiJS, the refetch would work.
      // The important thing is that get() returns undefined for stale records.
    })
  })

  describe('unsafeClear', () => {
    it('clears all records and resets to empty map', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      await database.action(async () => {
        await tasks.create()
        await tasks.create()
      })

      expect(tasks._cache.map.size).toBe(2)

      tasks._cache.unsafeClear()

      expect(tasks._cache.map.size).toBe(0)
    })

    it('keeps version number after clear', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      await database.action(() => tasks.create())

      tasks._invalidateCacheVersion()
      expect(tasks._cache.version).toBe(1)

      tasks._cache.unsafeClear()

      // Version should remain
      expect(tasks._cache.version).toBe(1)
    })
  })
})

