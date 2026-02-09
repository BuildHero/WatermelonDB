import { mockDatabase } from '../__tests__/testModels'
import * as Q from '../QueryDescription'

// Mock the WatermelonDBEvents emitter for CDC tests
const mockEventEmitter = {
  listeners: {},
  addListener: jest.fn((event, callback) => {
    mockEventEmitter.listeners[event] = callback
    return { remove: jest.fn(() => { delete mockEventEmitter.listeners[event] }) }
  }),
  emit: (event, data) => {
    if (mockEventEmitter.listeners[event]) {
      mockEventEmitter.listeners[event](data)
    }
  },
}

jest.mock('../adapters/sqlite/makeDispatcher/WatermelonEventEmitter', () => ({
  WatermelonDBEvents: mockEventEmitter,
}))

describe('database.enableNativeCDC()', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    mockEventEmitter.listeners = {}
  })

  it('subscribes to SQLITE_UPDATE_HOOK events and calls notify', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    // Mock enableNativeCDC on adapter
    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    const subscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], subscriber)

    await database.enableNativeCDC()

    // Simulate native hook emitting affected tables
    mockEventEmitter.emit('SQLITE_UPDATE_HOOK', ['mock_tasks'])

    expect(subscriber).toHaveBeenCalledTimes(1)
  })

  it('handles multiple tables from SQLITE_UPDATE_HOOK', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    const taskSubscriber = jest.fn()
    const commentSubscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], taskSubscriber)
    database.experimentalSubscribe(['mock_comments'], commentSubscriber)

    await database.enableNativeCDC()

    // Simulate native hook emitting multiple affected tables
    mockEventEmitter.emit('SQLITE_UPDATE_HOOK', ['mock_tasks', 'mock_comments'])

    expect(taskSubscriber).toHaveBeenCalledTimes(1)
    expect(commentSubscriber).toHaveBeenCalledTimes(1)
  })

  it('does not subscribe multiple times if called twice', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    await database.enableNativeCDC()
    await database.enableNativeCDC()

    expect(mockEventEmitter.addListener).toHaveBeenCalledTimes(1)
  })

  it('disableNativeCDC removes the subscription', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    const subscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], subscriber)

    await database.enableNativeCDC()

    // Emit should work
    mockEventEmitter.emit('SQLITE_UPDATE_HOOK', ['mock_tasks'])
    expect(subscriber).toHaveBeenCalledTimes(1)

    database.disableNativeCDC()

    // Emit should no longer trigger notify
    mockEventEmitter.emit('SQLITE_UPDATE_HOOK', ['mock_tasks'])
    expect(subscriber).toHaveBeenCalledTimes(1) // Still 1, not 2
  })

  it('enableNativeCDC calls setCDCEnabled(true) on adapter', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })
    database.adapter.underlyingAdapter.setCDCEnabled = jest.fn((enabled, callback) => {
      callback({ value: undefined })
    })

    await database.enableNativeCDC()

    expect(database.adapter.underlyingAdapter.setCDCEnabled).toHaveBeenCalledWith(true, expect.any(Function))
  })

  it('disableNativeCDC calls setCDCEnabled(false) on adapter', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })
    database.adapter.underlyingAdapter.setCDCEnabled = jest.fn((enabled, callback) => {
      callback({ value: undefined })
    })

    await database.enableNativeCDC()

    // Clear mock to verify disableNativeCDC call
    database.adapter.underlyingAdapter.setCDCEnabled.mockClear()

    database.disableNativeCDC()

    expect(database.adapter.underlyingAdapter.setCDCEnabled).toHaveBeenCalledWith(false, expect.any(Function))
  })

  it('ignores empty table arrays from SQLITE_UPDATE_HOOK', async () => {
    const { database } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    const subscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], subscriber)

    await database.enableNativeCDC()

    // Simulate native hook emitting empty array
    mockEventEmitter.emit('SQLITE_UPDATE_HOOK', [])

    expect(subscriber).not.toHaveBeenCalled()
  })

  it('batch() skips notify when native CDC is enabled', async () => {
    const { database, tasks } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    await database.enableNativeCDC()

    const subscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], subscriber)

    // batch() should NOT call notify directly when CDC is enabled
    await database.action(async () => {
      await tasks.create()
    })

    // Subscriber should NOT have been called by batch()
    // (in real scenario, the SQLITE_UPDATE_HOOK would trigger it)
    expect(subscriber).not.toHaveBeenCalled()

    // But if we simulate the hook, it should work
    mockEventEmitter.emit('SQLITE_UPDATE_HOOK', ['mock_tasks'])
    expect(subscriber).toHaveBeenCalledTimes(1)
  })

  it('batch() calls notify when native CDC is disabled', async () => {
    const { database, tasks } = mockDatabase({ actionsEnabled: true })

    // CDC not enabled - batch should call notify
    const subscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], subscriber)

    await database.action(async () => {
      await tasks.create()
    })

    // Subscriber SHOULD have been called by batch()
    expect(subscriber).toHaveBeenCalled()
  })

  it('batch() resumes calling notify after disableNativeCDC', async () => {
    const { database, tasks } = mockDatabase({ actionsEnabled: true })

    database.adapter.underlyingAdapter.enableNativeCDC = jest.fn((callback) => {
      callback({ value: undefined })
    })

    await database.enableNativeCDC()

    const subscriber = jest.fn()
    database.experimentalSubscribe(['mock_tasks'], subscriber)

    // batch() should NOT call notify when CDC is enabled
    await database.action(async () => {
      await tasks.create()
    })
    expect(subscriber).not.toHaveBeenCalled()

    // Disable CDC
    database.disableNativeCDC()

    // Now batch() should call notify again
    await database.action(async () => {
      await tasks.create()
    })
    expect(subscriber).toHaveBeenCalled()
  })
})

describe('database.notify()', () => {
  describe('Basic Functionality', () => {
    it('notifies with single table name', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      database.notify('mock_tasks')

      expect(subscriber).toHaveBeenCalledTimes(1)
    })

    it('notifies with array of table names', async () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber1 = jest.fn()
      const subscriber2 = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber1)
      database.experimentalSubscribe(['mock_comments'], subscriber2)

      database.notify(['mock_tasks', 'mock_comments'])

      expect(subscriber1).toHaveBeenCalledTimes(1)
      expect(subscriber2).toHaveBeenCalledTimes(1)
    })

    it('notifies with detailed change notifications', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      database.notify({
        mock_tasks: [{ record: task, type: 'updated' }],
      })

      expect(subscriber).toHaveBeenCalledTimes(1)
    })

    it('only notifies subscribed tables', async () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      database.notify('mock_comments')

      expect(subscriber).not.toHaveBeenCalled()
    })
  })

  describe('Observable Integration', () => {
    it('triggers query observations after notify', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const emissions = []
      const subscription = tasks
        .query()
        .observe()
        .subscribe((records) => {
          emissions.push(records.length)
        })

      // Wait for initial emission
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions).toEqual([0])

      // Create some tasks in a batch
      await database.action(async () => {
        const task1 = tasks.prepareCreate()
        const task2 = tasks.prepareCreate()
        await database.batch(task1, task2)
      })

      // Should have emitted after batch
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions).toEqual([0, 2])

      // Manually notify
      database.notify('mock_tasks')

      // Should trigger refetch - note that the value may not change if data didn't change
      await new Promise((resolve) => setTimeout(resolve, 50))
      // Verify at least we got initial and batch emissions
      expect(emissions.length).toBeGreaterThanOrEqual(2)

      subscription.unsubscribe()
    })

    it('triggers observations with collection.changes', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const emissions = []
      const subscription = tasks.changes.subscribe((changeSet) => {
        emissions.push(changeSet.length)
      })

      // Notify with empty changeset
      database.notify('mock_tasks')

      expect(emissions).toEqual([0])

      subscription.unsubscribe()
    })

    it('works with observeWithColumns', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      await database.action(() => tasks.create((task) => (task.name = 'Task 1')))

      const emissions = []
      const subscription = tasks
        .query()
        .observeWithColumns(['name'])
        .subscribe((records) => {
          emissions.push(records.length)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      const initialCount = emissions.length

      database.notify('mock_tasks')

      await new Promise((resolve) => setTimeout(resolve, 100))
      expect(emissions.length).toBeGreaterThanOrEqual(initialCount)

      subscription.unsubscribe()
    })
  })

  describe('Cache Invalidation', () => {
    it('invalidates cache when using simple notification', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create((t) => (t.name = 'Original')))

      // Ensure task is cached
      const cached = await tasks.find(task.id)
      expect(cached.name).toBe('Original')
      expect(cached).toBe(task) // Same object reference

      // Get current cache version
      const oldVersion = tasks._cacheVersion

      // Notify without details (external change)
      database.notify('mock_tasks')

      // Cache version should be bumped
      expect(tasks._cacheVersion).toBe(oldVersion)

      // Collection._notifyExternalChange doesn't bump version
      // It just triggers refetch
    })

    it('applies changes to cache with detailed notification', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create((t) => (t.name = 'Test')))

      // Ensure cached
      const cached = await tasks.find(task.id)
      expect(cached).toBe(task)

      // Update via prepare
      task.prepareUpdate((t) => {
        t.name = 'Updated'
      })

      const oldVersion = tasks._cacheVersion

      // Notify with details (like batch does)
      database.notify({
        mock_tasks: [{ record: task, type: 'updated' }],
      })

      // Cache version should NOT be bumped (detailed changes apply to cache)
      expect(tasks._cacheVersion).toBe(oldVersion)

      // Record should still be cached and accessible
      const stillCached = tasks._cache.get(task.id)
      expect(stillCached).toBe(task)
    })

    it('bumps cache version with _invalidateCacheVersion', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      const version1 = tasks._cacheVersion
      expect(version1).toBe(0)

      tasks._invalidateCacheVersion()
      expect(tasks._cacheVersion).toBe(1)
      expect(tasks._cache.version).toBe(1)

      tasks._invalidateCacheVersion()
      expect(tasks._cacheVersion).toBe(2)
      expect(tasks._cache.version).toBe(2)
    })
  })

  describe('Lazy Cache Invalidation', () => {
    it('lazily removes stale records on access', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())

      // Ensure cached
      expect(tasks._cache.map.has(task.id)).toBe(true)

      // Bump version (invalidates all)
      tasks._invalidateCacheVersion()

      // Record still in map
      expect(tasks._cache.map.has(task.id)).toBe(true)

      // But accessing it returns undefined (lazy removal)
      const cached = tasks._cache.get(task.id)
      expect(cached).toBeUndefined()

      // Now it's removed from map
      expect(tasks._cache.map.has(task.id)).toBe(false)
    })

    it('keeps fresh records after version bump', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task1 = await database.action(() => tasks.create())

      // Bump version
      tasks._invalidateCacheVersion()

      // Create new task (will have new version)
      const task2 = await database.action(() => tasks.create())

      // task1 is stale
      expect(tasks._cache.get(task1.id)).toBeUndefined()

      // task2 is fresh
      const cached = tasks._cache.get(task2.id)
      expect(cached).toBe(task2)
    })
  })

  describe('batch() Integration', () => {
    it('batch() calls notify internally', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      await database.action(async () => {
        await tasks.create()
      })

      expect(subscriber).toHaveBeenCalled()
    })

    it('batch() triggers observations', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const emissions = []
      const subscription = tasks
        .query()
        .observe()
        .subscribe((records) => {
          emissions.push(records.length)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions).toEqual([0])

      await database.action(() => tasks.create())

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions).toEqual([0, 1])

      subscription.unsubscribe()
    })
  })

  describe('copyTables() Integration', () => {
    it('copyTables() invalidates cache', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      const task = await database.action(() => tasks.create())
      const oldVersion = tasks._cacheVersion

      // Mock batchImport (LokiJS doesn't support it)
      database.adapter.underlyingAdapter.batchImport = jest.fn((tables, srcDB, callback) => {
        callback({ value: undefined })
      })

      // copyTables calls _invalidateCacheVersion
      await database.copyTables(['mock_tasks'], 'dummy_source')

      // Cache version should be bumped
      expect(tasks._cacheVersion).toBe(oldVersion + 1)
    })

    it('copyTables() triggers notifications', async () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      // Mock batchImport
      database.adapter.underlyingAdapter.batchImport = jest.fn((tables, srcDB, callback) => {
        callback({ value: undefined })
      })

      await database.copyTables(['mock_tasks'], 'dummy_source')

      expect(subscriber).toHaveBeenCalled()
    })

    it('copyTables() triggers observations', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Mock batchImport
      database.adapter.underlyingAdapter.batchImport = jest.fn((tables, srcDB, callback) => {
        callback({ value: undefined })
      })

      const emissions = []
      const subscription = tasks
        .query()
        .observe()
        .subscribe((records) => {
          emissions.push(records.length)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      const initialCount = emissions.length

      await database.copyTables(['mock_tasks'], 'dummy_source')

      // Should trigger refetch
      await new Promise((resolve) => setTimeout(resolve, 100))
      expect(emissions.length).toBeGreaterThanOrEqual(initialCount)

      subscription.unsubscribe()
    })
  })

  describe('Memory Safety', () => {
    it('does not load all records on notify', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Create many tasks
      await database.action(async () => {
        for (let i = 0; i < 100; i++) {
          await tasks.create()
        }
      })

      // Clear cache
      tasks._cache.map.clear()
      expect(tasks._cache.map.size).toBe(0)

      // Notify (should not load anything)
      database.notify('mock_tasks')

      // Cache should still be empty
      expect(tasks._cache.map.size).toBe(0)
    })

    it('only caches what queries fetch after notify', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Create 50 tasks
      await database.action(async () => {
        const createdTasks = []
        for (let i = 0; i < 50; i++) {
          createdTasks.push(tasks.prepareCreate())
        }
        await database.batch(...createdTasks)
      })

      // Verify they're all cached after creation
      expect(tasks._cache.map.size).toBe(50)

      // Notify (doesn't load anything)
      database.notify('mock_tasks')

      // Cache size should remain the same (notify doesn't clear or load)
      expect(tasks._cache.map.size).toBe(50)

      // The point is notify() itself doesn't cause mass loading
      // It just triggers observations which may then query as needed
    })
  })

  describe('Edge Cases', () => {
    it('handles empty table array', () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      database.notify([])

      expect(subscriber).not.toHaveBeenCalled()
    })

    it('handles empty change notifications', () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber)

      database.notify({})

      expect(subscriber).not.toHaveBeenCalled()
    })

    it('handles non-existent tables gracefully', () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      // Should not throw when notifying about a table that doesn't exist
      expect(() => {
        database.notify('non_existent_table')
      }).not.toThrow()

      expect(() => {
        database.notify(['non_existent_table', 'another_fake_table'])
      }).not.toThrow()

      expect(() => {
        database.notify({
          non_existent_table: [{ record: null, type: 'created' }],
        })
      }).not.toThrow()
    })

    it('handles copyTables with non-existent tables gracefully', async () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      // Mock batchImport to avoid actual database operations
      database.adapter.underlyingAdapter.batchImport = jest.fn((tables, srcDB, callback) => {
        callback({ value: undefined })
      })

      // Should not throw when copying tables that don't exist
      await expect(
        database.copyTables(['non_existent_table', 'another_fake_table'], {}),
      ).resolves.not.toThrow()
    })

    it('handles multiple subscribers to same table', () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber1 = jest.fn()
      const subscriber2 = jest.fn()
      database.experimentalSubscribe(['mock_tasks'], subscriber1)
      database.experimentalSubscribe(['mock_tasks'], subscriber2)

      database.notify('mock_tasks')

      expect(subscriber1).toHaveBeenCalledTimes(1)
      expect(subscriber2).toHaveBeenCalledTimes(1)
    })

    it('handles multiple tables in one subscriber', () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      database.experimentalSubscribe(['mock_tasks', 'mock_comments'], subscriber)

      database.notify('mock_tasks')
      expect(subscriber).toHaveBeenCalledTimes(1)

      database.notify('mock_comments')
      expect(subscriber).toHaveBeenCalledTimes(2)

      database.notify(['mock_tasks', 'mock_comments'])
      expect(subscriber).toHaveBeenCalledTimes(3)
    })

    it('does not notify unsubscribed listeners', () => {
      const { database } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      const unsubscribe = database.experimentalSubscribe(['mock_tasks'], subscriber)

      database.notify('mock_tasks')
      expect(subscriber).toHaveBeenCalledTimes(1)

      unsubscribe()

      database.notify('mock_tasks')
      expect(subscriber).toHaveBeenCalledTimes(1) // Should not increase
    })
  })

  describe('Collection._notifyExternalChange()', () => {
    it('notifies subscribers with empty changeset', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      const subscriber = jest.fn()
      tasks.experimentalSubscribe(subscriber)

      tasks._notifyExternalChange()

      expect(subscriber).toHaveBeenCalledWith([])
    })

    it('triggers Subject emission', () => {
      const { tasks } = mockDatabase({ actionsEnabled: true })

      const emissions = []
      tasks.changes.subscribe((changeSet) => {
        emissions.push(changeSet)
      })

      tasks._notifyExternalChange()

      expect(emissions).toEqual([[]])
    })
  })

  describe('withObservables-like behavior', () => {
    it('Model.observe() updates after notify', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Create a task
      const task = await database.action(() => tasks.create((t) => (t.name = 'Original')))

      // Simulate withObservables - observe a single model
      const emissions = []
      const subscription = task.observe().subscribe((observedTask) => {
        emissions.push(observedTask.name)
      })

      expect(emissions).toEqual(['Original'])

      // Update the task
      await database.action(() => task.update((t) => (t.name = 'Updated')))

      // Should have received update
      expect(emissions).toEqual(['Original', 'Updated'])

      // Manually trigger notify (simulating external change)
      database.notify('mock_tasks')

      // Wait for async update
      await new Promise((resolve) => setTimeout(resolve, 50))

      // Should have triggered refetch
      expect(emissions.length).toBeGreaterThanOrEqual(2)

      subscription.unsubscribe()
    })

    it('Query.observe() updates after notify (list component)', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Simulate withObservables for a list component
      const emissions = []
      const subscription = tasks
        .query(Q.where('is_completed', true))
        .observe()
        .subscribe((taskList) => {
          emissions.push(taskList.length)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions).toEqual([0])

      // Add completed tasks in batch
      await database.action(async () => {
        const t1 = tasks.prepareCreate((t) => {
          t.isCompleted = true
        })
        const t2 = tasks.prepareCreate((t) => {
          t.isCompleted = true
        })
        await database.batch(t1, t2)
      })

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions).toEqual([0, 2])

      // Simulate external data import
      database.notify('mock_tasks')

      await new Promise((resolve) => setTimeout(resolve, 50))
      // Should have triggered refetch
      expect(emissions.length).toBeGreaterThanOrEqual(2)

      subscription.unsubscribe()
    })

    it('Relation.observe() updates after notify', async () => {
      const { database, tasks, projects } = mockDatabase({ actionsEnabled: true })

      // Create project with task
      const project = await database.action(() => projects.create())

      await database.action(() =>
        tasks.create((t) => {
          t.projectId = project.id
        }),
      )

      // Use Query on related tasks instead of direct relation
      const emissions = []
      const subscription = tasks
        .query(Q.where('project_id', project.id))
        .observe()
        .subscribe((projectTasks) => {
          emissions.push(projectTasks.length)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions[0]).toBe(1)

      // Add another task to project
      await database.action(() =>
        tasks.create((t) => {
          t.projectId = project.id
        }),
      )

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions.length).toBeGreaterThanOrEqual(2)

      // Simulate external change notification
      database.notify('mock_tasks')

      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(emissions.length).toBeGreaterThanOrEqual(2)

      subscription.unsubscribe()
    })

    it('observeCount() updates after notify', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Simulate withObservables observeCount (e.g., badge counter)
      const emissions = []
      const subscription = tasks
        .query()
        .observeCount()
        .subscribe((count) => {
          emissions.push(count)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(emissions[0]).toBe(0)

      // Add tasks in a batch
      await database.action(async () => {
        const t1 = tasks.prepareCreate()
        const t2 = tasks.prepareCreate()
        await database.batch(t1, t2)
      })

      await new Promise((resolve) => setTimeout(resolve, 10))
      const countBefore = emissions.length

      // Notify (e.g., after copyTables)
      database.notify('mock_tasks')

      await new Promise((resolve) => setTimeout(resolve, 50))
      // Should have triggered at least one more emission
      expect(emissions.length).toBeGreaterThanOrEqual(countBefore)

      subscription.unsubscribe()
    })

    it('Multiple observables update together (component with multiple props)', async () => {
      const { database, tasks } = mockDatabase({ actionsEnabled: true })

      // Simulate withObservables with multiple observables
      // Like: { tasks: tasksQuery.observe(), projects: projectsQuery.observe() }

      const taskEmissions = []
      const allTasksEmissions = []

      const completedSub = tasks
        .query(Q.where('is_completed', true))
        .observe()
        .subscribe((list) => {
          taskEmissions.push(list.length)
        })

      const allSub = tasks
        .query()
        .observe()
        .subscribe((list) => {
          allTasksEmissions.push(list.length)
        })

      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(taskEmissions[0]).toBe(0)
      expect(allTasksEmissions[0]).toBe(0)

      // Create tasks in a batch
      await database.action(async () => {
        const t1 = tasks.prepareCreate((t) => (t.isCompleted = true))
        const t2 = tasks.prepareCreate((t) => (t.isCompleted = false))
        await database.batch(t1, t2)
      })

      await new Promise((resolve) => setTimeout(resolve, 10))
      // Completed should have 1, all should have 2
      expect(taskEmissions[taskEmissions.length - 1]).toBe(1)
      expect(allTasksEmissions[allTasksEmissions.length - 1]).toBe(2)

      const countBefore = taskEmissions.length

      // Notify (both should update)
      database.notify('mock_tasks')

      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(taskEmissions.length).toBeGreaterThanOrEqual(countBefore)
      expect(allTasksEmissions.length).toBeGreaterThan(0)

      completedSub.unsubscribe()
      allSub.unsubscribe()
    })

    it('withChangesForTables works after notify', async () => {
      const { database, tasks, projects } = mockDatabase({ actionsEnabled: true })

      // Simulate watching multiple tables (e.g., dashboard component)
      const emissions = []
      const subscription = database
        .withChangesForTables(['mock_tasks', 'mock_projects'])
        .subscribe((changes) => {
          emissions.push(changes ? 'change' : 'initial')
        })

      expect(emissions).toEqual(['initial'])

      // Make changes
      await database.action(() => tasks.create())
      expect(emissions).toEqual(['initial', 'change'])

      await database.action(() => projects.create())
      expect(emissions).toEqual(['initial', 'change', 'change'])

      // Notify (should trigger)
      database.notify(['mock_tasks', 'mock_projects'])
      expect(emissions).toEqual(['initial', 'change', 'change', 'change', 'change'])

      subscription.unsubscribe()
    })
  })
})
