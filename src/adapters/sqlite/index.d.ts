declare module '@BuildHero/watermelondb/adapters/sqlite' {
  import {
    AppSchema,
    DatabaseAdapter,
    Model,
    Query,
    RecordId,
    TableName,
  } from '@BuildHero/watermelondb'
  import {
    BatchOperation,
    CachedFindResult,
    CachedQueryResult,
  } from '@BuildHero/watermelondb/adapters/type'
  import { SchemaMigrations } from '@BuildHero/watermelondb/Schema/migrations'

  export type SQL = string

  export type SQLiteArg = string | boolean | number | null

  export type SQLiteQuery = [SQL, SQLiteArg[]]

  export interface SQLiteAdapterOptions {
    dbName?: string
    migrations?: SchemaMigrations
    schema: AppSchema
  }

  export default class SQLiteAdapter implements DatabaseAdapter {
    schema: AppSchema

    constructor(options: SQLiteAdapterOptions)

    enableHybridJSI(): void

    batch(operations: BatchOperation[]): Promise<void>

    count<T extends Model>(query: Query<T>): Promise<number>

    destroyDeletedRecords(tableName: TableName<any>, recordIds: RecordId[]): Promise<void>

    find(table: TableName<any>, id: RecordId): Promise<CachedFindResult>

    getDeletedRecords(tableName: TableName<any>): Promise<RecordId[]>

    getLocal(key: string): Promise<string | null>

    query<T extends Model>(query: Query<T>): Promise<CachedQueryResult>

    unsafeSqlQuery<T extends Model>(
      sql: string,
      tableName: TableName<T>,
    ): Promise<CachedQueryResult>

    removeLocal(key: string): Promise<void>

    setLocal(key: string, value: string): Promise<void>

    obliterateDatabase(): Promise<void>

    unsafeClearCachedRecords(): Promise<void>

    unsafeResetDatabase(): Promise<void>

    enableNativeCDC(): Promise<void>

    execSqlQuery(sql: string, params?: any[]): Promise<Record<string, any>[]>
  }
}
