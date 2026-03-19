import bridge from '../DatabaseBridge'
import DatabaseDriver from '../DatabaseDriver'

describe('execSqlQueryOnWriter', () => {
  const tag = 8889

  beforeAll(() => {
    const driver = new DatabaseDriver()
    const dbName = `file:writerTest_${process.pid}?mode=memory&cache=shared`
    driver.init(dbName)
    driver.database.execute('CREATE TABLE IF NOT EXISTS tasks (id varchar(16) primary key not null, name varchar(255), position integer)')
    driver.database.execute('CREATE TABLE IF NOT EXISTS local_storage (key varchar(16) primary key not null, value text not null)')
    driver.database.execute('DELETE FROM tasks')
    bridge.connections[tag] = { driver, synchronous: true, queue: [], status: 'connected' }
  })

  test('executes a SELECT query via writer', () => {
    bridge.execSqlQueryOnWriterSynchronous(
      tag,
      "INSERT INTO tasks (id, name, position) VALUES ('t1', 'Test Task', 1)",
      [],
    )

    const result = bridge.execSqlQueryOnWriterSynchronous(
      tag,
      'SELECT * FROM tasks WHERE id = ?',
      ['t1'],
    )

    expect(result.status).toBe('success')
    expect(result.result).toHaveLength(1)
    expect(result.result[0].id).toBe('t1')
    expect(result.result[0].name).toBe('Test Task')
  })

  test('executes INSERT via writer and reads back with regular query', () => {
    bridge.execSqlQueryOnWriterSynchronous(
      tag,
      "INSERT INTO tasks (id, name, position) VALUES ('t2', 'Writer Task', 2)",
      [],
    )

    const result = bridge.execSqlQuerySynchronous(
      tag,
      'SELECT * FROM tasks WHERE id = ?',
      ['t2'],
    )

    expect(result.status).toBe('success')
    expect(result.result).toHaveLength(1)
    expect(result.result[0].name).toBe('Writer Task')
  })

  test('passes params correctly', () => {
    bridge.execSqlQueryOnWriterSynchronous(
      tag,
      "INSERT INTO tasks (id, name, position) VALUES ('t3', 'Task A', 10)",
      [],
    )
    bridge.execSqlQueryOnWriterSynchronous(
      tag,
      "INSERT INTO tasks (id, name, position) VALUES ('t4', 'Task B', 20)",
      [],
    )

    const result = bridge.execSqlQueryOnWriterSynchronous(
      tag,
      'SELECT * FROM tasks WHERE position > ?',
      ['15'],
    )

    expect(result.status).toBe('success')
    expect(result.result).toHaveLength(1)
    expect(result.result[0].id).toBe('t4')
  })

  test('returns empty array for no results', () => {
    const result = bridge.execSqlQueryOnWriterSynchronous(
      tag,
      'SELECT * FROM tasks WHERE id = ?',
      ['nonexistent'],
    )

    expect(result.status).toBe('success')
    expect(result.result).toHaveLength(0)
  })

  test('produces same results as execSqlQuery', () => {
    bridge.execSqlQueryOnWriterSynchronous(
      tag,
      "INSERT INTO tasks (id, name, position) VALUES ('t5', 'Compare', 50)",
      [],
    )

    const writerResult = bridge.execSqlQueryOnWriterSynchronous(
      tag,
      'SELECT * FROM tasks WHERE id = ?',
      ['t5'],
    )
    const regularResult = bridge.execSqlQuerySynchronous(
      tag,
      'SELECT * FROM tasks WHERE id = ?',
      ['t5'],
    )

    expect(writerResult.status).toBe('success')
    expect(regularResult.status).toBe('success')
    expect(writerResult.result).toEqual(regularResult.result)
  })
})
