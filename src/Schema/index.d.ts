declare module '@BuildHero/watermelondb/Schema' {
  import { Model } from '@BuildHero/watermelondb'

  export type SchemaVersion = number

  export type TableName<T extends Model | void> = string
  export type ColumnName = string

  export function tableName<T extends Model>(name: string): TableName<T>

  export function columnName(name: string): ColumnName

  export type ColumnType = 'string' | 'number' | 'boolean'

  export interface ColumnSchema {
    name: ColumnName
    type: ColumnType
    isOptional?: boolean
    isIndexed?: boolean
  }

  interface ColumnMap {
    [name: string]: ColumnSchema
  }

  export type TableSchemaSpec = { name: TableName<any>; columns: ColumnSchema[] }

  export type FTS5TableSchemaSpect = {
    name: TableName<any>
    columns: ColumnName[]
    contentTable: TableName<any>
  }

  export interface FTS5TableSchema {
    name: TableName<any>
    columns: ColumnName[]
    contentTable: TableName<any>
  }

  export interface TableSchema {
    name: TableName<any>
    columns: ColumnMap
  }

  interface TableMap {
    [name: string]: TableSchema
  }

  export interface AppSchema {
    version: number
    tables: TableMap
  }

  export function appSchema(options: { version: number; tables: TableSchema[], fts5Tables: FTS5TableSchema[] }): AppSchema

  export function tableSchema(options: TableSchemaSpec): TableSchema

  export function fts5TableSchema(options: FTS5TableSchemaSpect): FTS5TableSchema
}
