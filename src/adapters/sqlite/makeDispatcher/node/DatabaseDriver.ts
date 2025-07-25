import Database from './Database'

function fixArgs(args: any): any {
  return Object.keys(args).reduce((acc, argName) => {
    if (typeof acc[argName] === 'boolean') {
      acc[argName] = acc[argName] ? 1 : 0
    }
    return acc
  }, args)
}

type Migrations = {
  from: number
  to: number
  sql: string
}

class MigrationNeededError extends Error {
  databaseVersion: number

  type: string

  constructor(databaseVersion: number) {
    super('MigrationNeededError')
    this.databaseVersion = databaseVersion
    this.type = 'MigrationNeededError'
    this.message = 'MigrationNeededError'
  }
}

class SchemaNeededError extends Error {
  type: string

  constructor() {
    super('SchemaNeededError')
    this.type = 'SchemaNeededError'
    this.message = 'SchemaNeededError'
  }
}

export function getPath(dbName: string): string {
  if (dbName === ':memory:' || dbName === 'file::memory:') {
    return dbName
  }

  let path =
    dbName.startsWith('/') || dbName.startsWith('file:') ? dbName : `${process.cwd()}/${dbName}`
  if (path.indexOf('.db') === -1) {
    if (path.indexOf('?') >= 0) {
      const index = path.indexOf('?')
      path = `${path.substring(0, index)}.db${path.substring(index)}`
    } else {
      path = `${path}.db`
    }
  }

  return path
}

class DatabaseDriver {
  static sharedMemoryConnections = {}

  database: Database

  cachedRecords: any = {}

  initialize = (dbName: string, schemaVersion: number) => {
    this.init(dbName)
    this.isCompatible(schemaVersion)
  }

  setUpWithSchema = (dbName: string, schema: string, schemaVersion: number) => {
    this.init(dbName)
    this.unsafeResetDatabase({ version: schemaVersion, sql: schema })
    this.isCompatible(schemaVersion)
  }

  setUpWithMigrations = (dbName: string, migrations: Migrations) => {
    this.init(dbName)
    this.migrate(migrations)
    this.isCompatible(migrations.to)
  }

  init = (dbName: string) => {
    this.database = new Database(getPath(dbName))

    const isSharedMemory = dbName.indexOf('mode=memory') > 0 && dbName.indexOf('cache=shared') > 0
    if (isSharedMemory) {
      // @ts-ignore
      if (!DatabaseDriver.sharedMemoryConnections[dbName]) {
        // @ts-ignore
        DatabaseDriver.sharedMemoryConnections[dbName] = this.database
      }
      // @ts-ignore
      this.database = DatabaseDriver.sharedMemoryConnections[dbName]
    }
  }

  find = (table: string, id: string) => {
    if (this.isCached(table, id)) {
      return id
    }

    const query = `SELECT * FROM '${table}' WHERE id == ? LIMIT 1`
    const results = this.database.queryRaw(query, [id])

    if (results.length === 0) {
      return null
    }

    this.markAsCached(table, id)
    return results[0]
  }

  cachedQuery = (table: string, query: string): any[] => {
    const results = this.database.queryRaw(query)
    return results.map((row: any) => {
      const id = `${row.id}`
      if (this.isCached(table, id)) {
        return id
      }
      this.markAsCached(table, id)
      return row
    })
  }

  query = (table: string, query: string) => this.cachedQuery(table, query)

  count = (query: string) => this.database.count(query)

  copyTables = (tables: string[], srcDB: string) => {
    this.database.execute(`ATTACH DATABASE '${srcDB}' as 'other'`)

    this.database.inTransaction(() => {
      tables.forEach(table => {
        this.database.execute(`INSERT OR IGNORE  INTO ${table} SELECT * FROM other.${table}`)
      })
    })

    this.database.execute('DETACH DATABASE \'other\'')
  }

  batch = (operations: any[]) => {
    const newIds: Array<any | Array<any>> = []
    const removedIds: Array<any | Array<any>> = []

    this.database.inTransaction(() => {
      operations.forEach((operation: any[]) => {
        const [type, table, ...rest] = operation
        switch (type) {
          case 'execute': {
            const [query, args] = rest
            this.database.execute(query, fixArgs(args))
            break
          }

          case 'create': {
            const [id, query, args] = rest
            this.database.execute(query, fixArgs(args))
            newIds.push([table, id])
            break
          }

          case 'markAsDeleted': {
            const [id] = rest
            const query = `UPDATE '${table}' SET _status='deleted' WHERE id == ?`
            this.database.execute(query, [id])
            removedIds.push([table, id])
            break
          }

          case 'destroyPermanently': {
            const [id] = rest
            // TODO: What's the behavior if nothing got deleted?
            this.database.execute(`DELETE FROM '${table}' WHERE id == ?`, [id])
            removedIds.push([table, id])
            break
          }

          default: {
            throw new Error('unreachable')
          }
        }
      })
    })

    newIds.forEach(([table, id]: [any, any]) => {
      this.markAsCached(table, id)
    })

    removedIds.forEach(([table, id]: [any, any]) => {
      this.removeFromCache(table, id)
    })
  }

  getDeletedRecords = (table: string): string[] => {
    return this.database
      .queryRaw(`SELECT ID FROM '${table}' WHERE _status='deleted'`)
      .map((row: any) => `${row.id}`)
  }

  destroyDeletedRecords = (table: string, records: string[]) => {
    const recordPlaceholders = records.map(() => '?').join(',')
    this.database.execute(`DELETE FROM '${table}' WHERE id IN (${recordPlaceholders})`, records)
  }

  // MARK: - LocalStorage

  getLocal = (key: string) => {
    const results = this.database.queryRaw('SELECT `value` FROM `local_storage` WHERE `key` = ?', [
      key,
    ])

    if (results.length > 0) {
      return results[0].value
    }

    return null
  }

  setLocal = (key: string, value: any) => {
    this.database.execute('INSERT OR REPLACE INTO `local_storage` (key, value) VALUES (?, ?)', [
      key,
      `${value}`,
    ])
  }

  removeLocal = (key: string) => {
    this.database.execute('DELETE FROM `local_storage` WHERE `key` == ?', [key])
  }

  // MARK: - Record caching

  hasCachedTable = (table: string) =>
    Object.prototype.hasOwnProperty.call(this.cachedRecords, table)

  isCached = (table: string, id: string) => {
    if (this.hasCachedTable(table)) {
      return this.cachedRecords[table].has(id)
    }
    return false
  }

  markAsCached = (table: string, id: string) => {
    if (!this.hasCachedTable(table)) {
      this.cachedRecords[table] = new Set()
    }
    this.cachedRecords[table].add(id)
  }

  removeFromCache = (table: string, id: string) => {
    if (this.hasCachedTable(table) && this.cachedRecords[table].has(id)) {
      this.cachedRecords[table].delete(id)
    }
  }

  // MARK: - Other private details

  isCompatible = (schemaVersion: number) => {
    const databaseVersion = this.database.userVersion
    if (schemaVersion !== databaseVersion) {
      if (databaseVersion > 0 && databaseVersion < schemaVersion) {
        throw new MigrationNeededError(databaseVersion)
      } else {
        throw new SchemaNeededError()
      }
    }
  }

  unsafeResetDatabase = (schema: { sql: string; version: number }) => {
    this.database.unsafeDestroyEverything()
    this.cachedRecords = {}

    this.setUpSchema(schema)
  }

  obliterateDatabase = () => {
    this.database.obliterateDatabase()
  }

  setUpSchema = (schema: { sql: string; version: number }) => {
    this.database.inTransaction(() => {
      this.database.executeStatements(schema.sql + this.localStorageSchema)
      this.database.userVersion = schema.version
    })
  }

  migrate = (migrations: Migrations) => {
    const databaseVersion = this.database.userVersion

    if (`${databaseVersion}` !== `${migrations.from}`) {
      throw new Error(
        `Incompatbile migration set applied. DB: ${databaseVersion}, migration: ${migrations.from}`,
      )
    }

    this.database.inTransaction(() => {
      this.database.executeStatements(migrations.sql)
      this.database.userVersion = migrations.to
    })
  }

  execSqlQuery = (query: string, params: any[]) => {
    try {
      return this.database.queryRaw(query, params)
    } catch (_) {
      return this.database.execute(query, params)
    }
  }

  setUpdateHook = (updateHook: any) => {
    this.database.setUpdateHook(updateHook)
  }

  localStorageSchema: string = `
      create table local_storage (
      key varchar(16) primary key not null,
      value text not null
      );

      create index local_storage_key_index on local_storage (key);
      `
}

export default DatabaseDriver
