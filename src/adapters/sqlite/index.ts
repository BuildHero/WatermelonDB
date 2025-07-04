/* eslint-disable global-require */

import {connectionTag, ConnectionTag, logger, invariant} from '../../utils/common';
import { ResultCallback, mapValue, toPromise, fromPromise } from '../../utils/fp/Result'

import type { RecordId } from '../../Model'
import type { SerializedQuery } from '../../Query'
import type { TableName, AppSchema, SchemaVersion } from '../../Schema'
import type { SchemaMigrations, MigrationStep } from '../../Schema/migrations'
import type {
  DatabaseAdapter,
  SQLDatabaseAdapter,
  CachedQueryResult,
  CachedFindResult,
  BatchOperation,
} from '../type'
import {
  sanitizeFindResult,
  sanitizeQueryResult,
  devSetupCallback,
  validateAdapter,
  validateTable,
} from '../common'
import type {
  DispatcherType,
  SQL,
  SQLiteAdapterOptions,
  SQLiteArg,
  SQLiteQuery,
  NativeBridgeBatchOperation,
  NativeDispatcher,
} from './type'

import encodeQuery from './encodeQuery'
import encodeUpdate from './encodeUpdate'
import encodeInsert from './encodeInsert'

let makeDispatcher: any = null
let DatabaseBridge: any = null
let getDispatcherType: any = null

export type { SQL, SQLiteArg, SQLiteQuery }

// Hacky-ish way to create an object with NativeModule-like shape, but that can dispatch method
// calls to async, synch NativeModule, or JSI implementation w/ type safety in rest of the impl

export default class SQLiteAdapter implements DatabaseAdapter, SQLDatabaseAdapter {
  schema: AppSchema;

  migrations: SchemaMigrations | null | undefined;

  _tag: ConnectionTag = connectionTag();

  _dbName: string;

  _dispatcherType: DispatcherType;

  _dispatcher: NativeDispatcher;

  _initPromise: Promise<undefined>;

  _hybridJSIEnabled: boolean = false;

  constructor(options: SQLiteAdapterOptions) {
    const dispatcher = require('./makeDispatcher')

    makeDispatcher = dispatcher.makeDispatcher
    DatabaseBridge = dispatcher.DatabaseBridge
    getDispatcherType = dispatcher.getDispatcherType

    // console.log(`---> Initializing new adapter (${this._tag})`)
    const { dbName, schema, migrations } = options
    this.schema = schema
    this.migrations = migrations
    this._dbName = this._getName(dbName)

    this._dispatcherType = getDispatcherType(options)
    this._dispatcher = makeDispatcher(this._dispatcherType, this._tag, this._dbName)

    if (process.env.NODE_ENV !== 'production') {
      invariant(
        !('migrationsExperimental' in options),
        'SQLiteAdapter `migrationsExperimental` option has been renamed to `migrations`',
      )
      invariant(
        DatabaseBridge,
        `NativeModules.DatabaseBridge is not defined! This means that you haven't properly linked WatermelonDB native module. Refer to docs for more details`,
      )
      // @ts-ignore
      validateAdapter(this)
    }

    // @ts-ignore
    this._initPromise = this._init().then(() => {
      if ('onReady' in options) {
        const {onReady} = options

        if (typeof onReady === 'function') {
          onReady()
        }
      }
    })

    fromPromise(this._initPromise, devSetupCallback)
  }

  get initializingPromise(): Promise<void> {
    return this._initPromise
  }

  async testClone(options: Partial<SQLiteAdapterOptions> = {}): Promise<SQLiteAdapter> {
    const clone = new SQLiteAdapter({
      dbName: this._dbName,
      schema: this.schema,
      synchronous: this._dispatcherType === 'synchronous',
      experimentalUseJSI: this._dispatcherType === 'jsi',
      ...(this.migrations ? { migrations: this.migrations } : {}),
      ...options,
    })

    invariant(
      clone._dispatcherType === this._dispatcherType,
      'testCloned adapter has bad dispatcher type',
    )
    await clone._initPromise
    return clone
  }

  _getName(name?: string | null): string {
    if (process.env.NODE_ENV === 'test') {
      return name || `file:testdb${this._tag}?mode=memory&cache=shared`
    }

    return name || 'watermelon'
  }

  async _init(): Promise<void> {
    // Try to initialize the database with just the schema number. If it matches the database,
    // we're good. If not, we try again, this time sending the compiled schema or a migration set
    // This is to speed up the launch (less to do and pass through bridge), and avoid repeating
    // migration logic inside native code
    const status = await toPromise(callback =>
      this._dispatcher.initialize(this._dbName, this.schema.version, callback),
    )

    // NOTE: Race condition - logic here is asynchronous, but synchronous-mode adapter does not allow
    // for queueing operations. will fail if you start making actions immediately
    if ((status as any).code === 'schema_needed') {
      await this._setUpWithSchema()
    } else if ((status as any).code === 'migrations_needed') {
      await this._setUpWithMigrations((status as any).databaseVersion)
    } else {
      invariant((status as any).code === 'ok', 'Invalid database initialization status')
    }

    // console.log(`---> Done initializing (${this._tag})`)
  }

  async _setUpWithMigrations(databaseVersion: SchemaVersion): Promise<void> {
    logger.log('[WatermelonDB][SQLite] Database needs migrations')
    invariant(databaseVersion > 0, 'Invalid database schema version')

    const migrationSteps = this._migrationSteps(databaseVersion)

    if (migrationSteps) {
      logger.log(
        `[WatermelonDB][SQLite] Migrating from version ${databaseVersion} to ${this.schema.version}...`,
      )

      try {
        await toPromise(callback =>
          this._dispatcher.setUpWithMigrations(
            this._dbName,
            this._encodeMigrations(migrationSteps),
            databaseVersion,
            this.schema.version,
            callback,
          ),
        )
        logger.log('[WatermelonDB][SQLite] Migration successful')
      } catch (error: any) {
        logger.error('[WatermelonDB][SQLite] Migration failed', error)
        throw error
      }
    } else {
      logger.warn(
        '[WatermelonDB][SQLite] Migrations not available for this version range, resetting database instead',
      )
      await this._setUpWithSchema()
    }
  }

  async _setUpWithSchema(): Promise<void> {
    logger.log(
      `[WatermelonDB][SQLite] Setting up database with schema version ${this.schema.version}`,
    )
    await toPromise(callback =>
      this._dispatcher.setUpWithSchema(
        this._dbName,
        this._encodedSchema(),
        this.schema.version,
        callback,
      ),
    )
    logger.log(`[WatermelonDB][SQLite] Schema set up successfully`)
  }

  find(
    table: TableName<any>,
    id: RecordId,
    callback: ResultCallback<CachedFindResult>,
  ): void {
    validateTable(table, this.schema)
    this._dispatcher.find(table, id, result =>
      callback(
        mapValue(rawRecord => sanitizeFindResult(rawRecord, this.schema.tables[table] as any), result),
      ),
    )
  }

  enableHybridJSI(): void {
    if (!makeDispatcher) {
      const dispatcher = require('./makeDispatcher')
      makeDispatcher = dispatcher.makeDispatcher
    }
    this._dispatcher = makeDispatcher(this._dispatcherType, this._tag, this._dbName, true)
    this._hybridJSIEnabled = true
  }

  query(query: SerializedQuery, callback: ResultCallback<CachedQueryResult>): void {
    validateTable(query.table, this.schema)
    this.unsafeSqlQuery(query.table, encodeQuery(query, false, this.schema), callback)
  }

  // @ts-ignore
  execSqlQuery(sql: string, params: any[], callback: ResultCallback<CachedQueryResult>): void {
    this._dispatcher.execSqlQuery(sql, params?.map((param: any) => `${param}`), result => callback(result))
  }

  unsafeSqlQuery(
    table: TableName<any>,
    sql: string,
    callback: ResultCallback<CachedQueryResult>,
  ): void {
    validateTable(table, this.schema)
    this._dispatcher.query(table, sql, result =>
      callback(
        mapValue((rawRecords: any) => sanitizeQueryResult(rawRecords, this.schema.tables[table] as any), result),
      ),
    )
  }

  count(query: SerializedQuery, callback: ResultCallback<number>): void {
    validateTable(query.table, this.schema)
    const sql = encodeQuery(query, true, this.schema)
    this._dispatcher.count(sql, callback)
  }

  batchImport(tables: any, srcDB: any, callback: any): void {
    this._dispatcher.copyTables(tables, srcDB, callback)
  }

  batch(operations: BatchOperation[], callback: ResultCallback<undefined>): void {
    // @ts-ignore
    const batchOperations: NativeBridgeBatchOperation[] = operations.map(operation => {
      const [type, table, rawOrId] = operation
      validateTable(table, this.schema)
      switch (type) {
        case 'create': {
          // @ts-ignore
          return ['create', table, rawOrId.id].concat(encodeInsert(table, rawOrId));
        }
        case 'update': {
          // @ts-ignore
          return ['execute', table].concat(encodeUpdate(table, rawOrId));
        }
        case 'markAsDeleted':
        case 'destroyPermanently':
          return operation; // same format, no need to repack
        default:
          throw new Error('unknown batch operation type')
      }
    })
    const { batchJSON } = this._dispatcher
    if (batchJSON) {
      batchJSON(JSON.stringify(batchOperations), callback)
    } else {
      this._dispatcher.batch(batchOperations, callback)
    }
  }

  getDeletedRecords(table: TableName<any>, callback: ResultCallback<RecordId[]>): void {
    validateTable(table, this.schema)
    this._dispatcher.getDeletedRecords(table, callback)
  }

  destroyDeletedRecords(
    table: TableName<any>,
    recordIds: RecordId[],
    callback: ResultCallback<undefined>,
  ): void {
    validateTable(table, this.schema)
    this._dispatcher.destroyDeletedRecords(table, recordIds, callback)
  }

  unsafeResetDatabase(callback: ResultCallback<undefined>): void {
    this._dispatcher.unsafeResetDatabase(this._encodedSchema(), this.schema.version, (result: any) => {
      if (result.value) {
        logger.log('[WatermelonDB][SQLite] Database is now reset')
      }
      callback(result)
    })
  }

  obliterateDatabase(callback: ResultCallback<undefined>): void {
    this._dispatcher.obliterateDatabase(callback)
  }

  getLocal(key: string, callback: ResultCallback<string | null | undefined>): void {
    this._dispatcher.getLocal(key, callback)
  }

  setLocal(key: string, value: string, callback: ResultCallback<undefined>): void {
    this._dispatcher.setLocal(key, value, callback)
  }

  removeLocal(key: string, callback: ResultCallback<undefined>): void {
    this._dispatcher.removeLocal(key, callback)
  }

  enableNativeCDC(callback: ResultCallback<undefined>): void {
    this._dispatcher.enableNativeCDC(callback)
  }

  _encodedSchema(): SQL {
    const { encodeSchema } = require('./encodeSchema')
    return encodeSchema(this.schema)
  }

  _migrationSteps(fromVersion: SchemaVersion): MigrationStep[] | null | undefined {
    const { stepsForMigration } = require('../../Schema/migrations/stepsForMigration')
    const { migrations } = this
    // TODO: Remove this after migrations are shipped
    if (!migrations) {
      return null
    }
    return stepsForMigration({
      migrations,
      fromVersion,
      toVersion: this.schema.version,
    })
  }

  _encodeMigrations(steps: MigrationStep[]): SQL {
    const { encodeMigrationSteps } = require('./encodeSchema')
    return encodeMigrationSteps(steps)
  }
}
