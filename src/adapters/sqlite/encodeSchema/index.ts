import {keys, values} from 'rambdax';
import type {
  FTS5TableSchema,
  TableSchema,
  AppSchema,
  ColumnSchema,
  TableName,
} from '../../../Schema'
import { nullValue } from '../../../RawRecord'
import type {
  MigrationStep,
  CreateTableMigrationStep,
  AddColumnsMigrationStep,
  DropTableMigrationStep,
  DropColumnsMigrationStep,
  AddIndexMigrationStep,
  RemoveIndexMigrationStep,
  DropFTS5TableMigrationStep,
  CreateFTS5TableMigrationStep,
} from '../../../Schema/migrations'
import type { SQL } from '../index'

import encodeName from '../encodeName'
import encodeValue from '../encodeValue'
import { tableName } from '../../../Schema'

const standardColumns = `"id" primary key, "_changed", "_status"`

const encodeCreateTable: (arg1: TableSchema) => SQL = ({ name, columns }) => {
  const columnsSQL = [standardColumns]
    .concat(keys(columns).map(column => encodeName(column)))
    .join(', ')
  return `create table ${encodeName(name)} (${columnsSQL});`
}

const encodeCreateFTS5Table: (arg1: FTS5TableSchema) => SQL = ({ name, columns, contentTable }) => {
  const columnsSQL = columns.map(column => encodeName(column)).join(', ')

  return `
    create virtual table ${encodeName(name)} using fts5(id, ${columnsSQL}, prefix ='2 3 4');
    insert or replace into ${encodeName(
      name,
    )} (rowid, id, ${columnsSQL}) select rowid, id, ${columnsSQL} from ${contentTable};
  `
}

const encodeFTS5SyncProcedures = ({ name, columns, contentTable }: { name: string, columns: string[], contentTable: string }) => {
  const columnsSQL = columns.map(column => encodeName(column)).join(', ')

  const newColumnsSQL = columns.map(column => `new.${encodeName(column)}`).join(', ')

  return `
    create trigger ${encodeName(`${name}_ai`)} after insert on ${encodeName(contentTable)} begin
      insert or replace into ${encodeName(
        name,
      )} (rowid, id, ${columnsSQL}) values (new.rowid, new.id, ${newColumnsSQL});
    end;

    create trigger ${encodeName(`${name}_ad`)} after delete on ${encodeName(contentTable)} begin
      delete from ${encodeName(name)} where id = old.id;
    end;

    create trigger ${encodeName(`${name}_au`)} after update on ${encodeName(contentTable)} begin
      insert or replace into ${encodeName(
        name,
      )} (rowid, id, ${columnsSQL}) values (new.rowid, new.id, ${newColumnsSQL});
    end;
  `
}

const encodeDropFTS5Table: (arg1: FTS5TableSchema) => SQL = ({ name }) =>
  `drop table if exists ${encodeName(name)};`

const encodeDropFTS5SyncProcedures = ({ name }: { name: string }) => {
  return `
    drop trigger if exists ${encodeName(`${name}_ai`)};
    drop trigger if exists ${encodeName(`${name}_ad`)};
    drop trigger if exists ${encodeName(`${name}_au`)};
  `
}

const encodeFTS5Table: (arg1: FTS5TableSchema) => SQL = tableSchema =>
  encodeCreateFTS5Table(tableSchema) +
  encodeFTS5SyncProcedures(tableSchema)
    .replaceAll(/[\r\n\t]/g, '')
    .replaceAll(/\s{2,}/g, ' ')
    .replaceAll(/;\s*/gm, ';')

const encodeIndex: (arg1: ColumnSchema, arg2: TableName<any>) => SQL = (column, tableName) =>
  column.isIndexed
    ? `create index "${tableName}_${column.name}" on ${encodeName(tableName)} (${encodeName(
        column.name,
      )});`
    : ''

const encodeTableIndicies: (arg1: TableSchema) => SQL = ({ name: tableName, columns }) =>
  values(columns)
    .map(column => encodeIndex((column as any), tableName))
    .concat([`create index "${tableName}__status" on ${encodeName(tableName)} ("_status");`])
    .join('')

const transform = (sql: string, transformer?: ((arg1: string) => string) | null) =>
  transformer ? transformer(sql) : sql

const encodeTable: (arg1: TableSchema) => SQL = table =>
  transform(encodeCreateTable(table) + encodeTableIndicies(table), table.unsafeSql)

export const encodeSchema: (arg1: AppSchema) => SQL = ({ tables, fts5Tables, unsafeSql }) => {
  const sql = values(tables)
    .map(encodeTable as any)
    .join('')

  const fts5Sql = values(fts5Tables as any)
    .map(encodeFTS5Table)
    .join('');

  return transform(sql + fts5Sql, unsafeSql)
}

const encodeDropFTS5TableMigrationStep: (arg1: DropFTS5TableMigrationStep) => SQL = ({ name }) => {
  const tableNameTyped = tableName(name)
  // @ts-ignore
  return (encodeDropFTS5Table({ name: tableNameTyped }) + encodeDropFTS5SyncProcedures({ name: tableNameTyped }));
};

const encodeCreateFTS5TableMigrationStep: (arg1: CreateFTS5TableMigrationStep) => SQL = ({ schema }) =>
  encodeFTS5Table(schema)

const encodeCreateTableMigrationStep: (arg1: CreateTableMigrationStep) => SQL = ({ schema }) =>
  encodeTable(schema)

const encodeAddColumnsMigrationStep: (arg1: AddColumnsMigrationStep) => SQL = ({
  table,
  columns,
  unsafeSql,
}) =>
  columns
    .map(column => {
      const addColumn = `alter table ${encodeName(table)} add ${encodeName(column.name)};`
      const setDefaultValue = `update ${encodeName(table)} set ${encodeName(
        column.name,
      )} = ${encodeValue(nullValue(column))};`
      const addIndex = encodeIndex(column, table)

      return transform(addColumn + setDefaultValue + addIndex, unsafeSql)
    })
    .join('')

const encodeDropTableMigrationStep: (arg1: DropTableMigrationStep) => SQL = ({ table, unsafeSql }) => {
  const sql = `drop table if exists ${encodeName(table)};`
  return transform(sql, unsafeSql)
}

const encodeDropColumnsMigrationStep: (arg1: DropColumnsMigrationStep) => SQL = ({
  table,
  columns,
  unsafeSql,
}) => {
  // SQLite 3.35.0+ supports DROP COLUMN
  const sql = columns
    .map(column => `alter table ${encodeName(table)} drop column ${encodeName(column)};`)
    .join('')
  return transform(sql, unsafeSql)
}

const encodeAddIndexMigrationStep: (arg1: AddIndexMigrationStep) => SQL = ({
  table,
  column,
  unsafeSql,
}) => {
  const sql = `create index if not exists "${table}_${column}" on ${encodeName(
    table,
  )} (${encodeName(column)});`
  return transform(sql, unsafeSql)
}

const encodeRemoveIndexMigrationStep: (arg1: RemoveIndexMigrationStep) => SQL = ({
  table,
  column,
  unsafeSql,
}) => {
  const sql = `drop index if exists "${table}_${column}";`
  return transform(sql, unsafeSql)
}

export const encodeMigrationSteps: (arg1: MigrationStep[]) => SQL = steps =>
  steps
    .map(step => {
      if (step.type === 'create_table') {
        return encodeCreateTableMigrationStep(step)
      }
      if (step.type === 'create_fts5_table') {
        return encodeCreateFTS5TableMigrationStep(step)
      } else if (step.type === 'add_columns') {
        return encodeAddColumnsMigrationStep(step)
      } else if (step.type === 'sql') {
        return step.sql
      } else if (step.type === 'drop_fts5_table') {
        return encodeDropFTS5TableMigrationStep(step)
      } else if (step.type === 'drop_table') {
        return encodeDropTableMigrationStep(step)
      } else if (step.type === 'drop_columns') {
        return encodeDropColumnsMigrationStep(step)
      } else if (step.type === 'add_index') {
        return encodeAddIndexMigrationStep(step)
      } else if (step.type === 'remove_index') {
        return encodeRemoveIndexMigrationStep(step)
      }

      throw new Error(`Unsupported migration step ${(step as any).type}`)
    })
    .join('')
