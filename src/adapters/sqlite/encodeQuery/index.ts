/* eslint-disable no-use-before-define */

import { invariant } from '../../../utils/common'
import type { SerializedQuery, QueryAssociation } from '../../../Query'
import type {
  NonNullValues,
  Operator,
  Where,
  ComparisonRight,
  Comparison,
  SortBy,
  QueryDescription,
} from '../../../QueryDescription'
import * as Q from '../../../QueryDescription'
import { TableName, ColumnName } from '../../../Schema'

import encodeValue from '../encodeValue'
import encodeName from '../encodeName'

function mapJoin<T>(array: T[], mapper: (arg1: T) => string, joiner: string): string {
  // NOTE: DO NOT try to optimize this by concatenating strings together. In non-JIT JSC,
  // concatenating strings is extremely slow (5000ms vs 120ms on 65K sample)
  return array.map(mapper).join(joiner)
}

const encodeValues: (arg1: NonNullValues) => string = (values) =>
  `(${mapJoin(values as any[], encodeValue, ', ')})`

const getComparisonRight = (table: TableName<any>, comparisonRight: ComparisonRight): string => {
  if ((comparisonRight as any).values) {
    return encodeValues((comparisonRight as any).values)
  } else if ((comparisonRight as any).column) {
    return `${encodeName(table)}.${encodeName((comparisonRight as any).column)}`
  }

  return typeof (comparisonRight as any).value !== 'undefined'
    ? encodeValue((comparisonRight as any).value)
    : 'null'
}

// Note: it's necessary to use `is` / `is not` for NULL comparisons to work correctly
// See: https://sqlite.org/lang_expr.html
const operators: Partial<Record<Operator, string>> = {
  eq: 'is',
  notEq: 'is not',
  gt: '>',
  gte: '>=',
  weakGt: '>', // For non-column comparison case
  lt: '<',
  lte: '<=',
  oneOf: 'in',
  notIn: 'not in',
  between: 'between',
  like: 'like',
  notLike: 'not like',
} as any

const encodeComparison = (table: TableName<any>, comparison: Comparison) => {
  if (comparison.operator === 'between') {
    const { right } = comparison
    return (right as any).values
      ? `between ${encodeValue((right as any).values[0])} and ${encodeValue((right as any).values[1])}`
      : ''
  }

  return `${operators[comparison.operator]} ${getComparisonRight(table, comparison.right)}`
}

const encodeWhere =
  (table: TableName<any>, associations: QueryAssociation[]) =>
  (where: Where): string => {
    switch (where.type) {
      case 'and':
        return `(${encodeAndOr(associations, 'and', table, where.conditions)})`
      case 'or':
        return `(${encodeAndOr(associations, 'or', table, where.conditions)})`
      case 'where':
        return encodeWhereCondition(associations, table, where.left, where.comparison)
      case 'on':
        invariant(
          associations.some(({ to }) => to === where.table),
          'To nest Q.on inside Q.and/Q.or you must explicitly declare Q.experimentalJoinTables at the beginning of the query',
        )
        return `(${encodeAndOr(associations, 'and', where.table, where.conditions)})`
      case 'sql':
        return where.expr
      default:
        throw new Error(`Unknown clause ${where.type}`)
    }
  }

const encodeWhereCondition = (
  associations: QueryAssociation[],
  table: TableName<any>,
  left: ColumnName,
  comparison: Comparison,
): string => {
  // if right operand is a value, we can use simple comparison
  // if a column, we must check for `not null > null`
  if (comparison.operator === 'weakGt' && (comparison.right as any).column) {
    return encodeWhere(
      table,
      associations,
    )(
      Q.or(
        Q.where(left, Q.gt(Q.column((comparison.right as any).column))),
        Q.and(Q.where(left, Q.notEq(null)), Q.where((comparison.right as any).column, null)),
      ),
    )
  }

  return `${encodeName(table)}.${encodeName(left)} ${encodeComparison(table, comparison)}`
}

const encodeAndOr = (
  associations: QueryAssociation[],
  op: string,
  table: TableName<any>,
  conditions: Where[],
) => {
  if (conditions.length) {
    return mapJoin(conditions, encodeWhere(table, associations), ` ${op} `)
  }
  return ''
}

const andJoiner = ' and '

const encodeConditions = (
  table: TableName<any>,
  description: QueryDescription,
  associations: QueryAssociation[],
): string => {
  const clauses = mapJoin(description.where, encodeWhere(table, associations), andJoiner)

  return clauses.length ? ` where ${clauses}` : ''
}

const encodeEagerMethod = (
  table: TableName<any>,
  countMode: boolean | null | undefined = false,
  associations: QueryAssociation[],
  schema: any,
): string => {
  const eagerTables = Array.from(
    new Set(
      [{ table, alias: undefined }].concat(
        // @ts-ignore
        associations.map(({ to, joinedAs, info: { aliasFor } }: QueryAssociation) => ({
          table: aliasFor || to,
          alias: joinedAs,
        })),
      ),
    ),
  )

  const getTableColumns = (tableName: string) =>
    ['id', '_changed', '_status'].concat(Object.keys(schema.tables[tableName].columns))

  const selectList = eagerTables
    .map(({ table, alias }) => {
      return getTableColumns(table)
        .map((column) => {
          return `${encodeName(alias || table)}.${encodeName(column)} as ${encodeName(
            `${alias || table}.${column}`,
          )}`
        })
        .join(', ')
    })
    .join(', ')

  if (countMode) {
    return `select count(distinct ${encodeName(table)}."id") as "count" from ${encodeName(table)}`
  }

  return `select ${selectList} from ${encodeName(table)}`
}

// If query contains `on()` conditions on tables with which the primary table has a has-many
// relation, then we need to add `distinct` on the query to ensure there are no duplicates
const encodeMethod = (
  table: TableName<any>,
  countMode: boolean,
  needsDistinct: boolean,
): string => {
  if (countMode) {
    return needsDistinct
      ? `select count(distinct ${encodeName(table)}."id") as "count" from ${encodeName(table)}`
      : `select count(*) as "count" from ${encodeName(table)}`
  }

  return needsDistinct
    ? `select distinct ${encodeName(table)}.* from ${encodeName(table)}`
    : `select ${encodeName(table)}.* from ${encodeName(table)}`
}

const encodeAssociation =
  (description: QueryDescription) =>
  ({
    from: mainTable,
    to: joinedTable,
    info: association,
    joinedAs: alias,
  }: QueryAssociation): string => {
    // TODO: We have a problem here. For all of eternity, WatermelonDB Q.ons were encoded using JOIN
    // However, this precludes many legitimate use cases for Q.ons once you start nesting them
    // (e.g. get tasks where X or has a tag assignment that Y -- if there is no tag assignment, this will
    // fail to join)
    // LEFT JOIN needs to be used to address this… BUT technically that's a breaking change. I never
    // considered a possiblity of making a query like `Q.on(relation_id, x != 'bla')`. Before this would
    // only match if there IS a relation, but with LEFT JOIN it would also match if record does not have
    // this relation. I don't know if there are legitimate use cases where this would change anything
    // so I need more time to think about whether this breaking change is OK to make or if we need to
    // do something more clever/add option/whatever.
    // so for now, i'm making an extreeeeemelyyyy bad hack to make sure that there's no breaking change
    // for existing code and code with nested Q.ons probably works (with caveats)

    const actualJoinedTable = association?.aliasFor || joinedTable
    const joinedAs = alias || actualJoinedTable

    const usesOldJoinStyle = description.where.some(
      (clause) => clause.type === 'on' && clause.table === actualJoinedTable,
    )
    const joinKeyword = usesOldJoinStyle ? ' join ' : ' left join '
    const joinBeginning = `${joinKeyword}${encodeName(actualJoinedTable)} ${encodeName(
      joinedAs,
    )} on ${encodeName(joinedAs)}.`

    return association.type === 'belongs_to'
      ? `${joinBeginning}"id" = ${encodeName(mainTable)}.${encodeName(association.key)}`
      : `${joinBeginning}${encodeName(association.foreignKey)} = ${encodeName(mainTable)}."id"`
  }

const encodeJoin = (description: QueryDescription, associations: QueryAssociation[]): string =>
  associations.length ? associations.map(encodeAssociation(description)).join('') : ''

const encodeOrderBy = (table: TableName<any>, sortBys: SortBy[]) => {
  if (sortBys.length === 0) {
    return ''
  }
  const orderBys = sortBys
    .map((sortBy) => {
      return `${encodeName(table)}.${encodeName(sortBy.sortColumn)} ${sortBy.sortOrder}`
    })
    .join(', ')
  return ` order by ${orderBys}`
}

const encodeLimitOffset = (limit?: number | null, offset?: number | null) => {
  if (!limit) {
    return ''
  }
  const optionalOffsetStmt = offset ? ` offset ${offset}` : ''

  return ` limit ${limit}${optionalOffsetStmt}`
}

const encodeCTE = (description: any, sql: string, table: string) => {
  return `
      with ${table}_cte as (
        ${description.cte}
      )

      ${sql.replace(new RegExp(table, 'g'), `${table}_cte`)}
    `
}

const encodeQuery = (
  query: SerializedQuery,
  countMode: boolean = false,
  schema: any = null,
): string => {
  const { table, description, associations } = query

  if (description.sql) {
    return description.sql
  }

  const hasToManyJoins = associations.some(({ info }) => info.type === 'has_many')

  description.take &&
    invariant(
      !countMode,
      'take/skip is not currently supported with counting. Please contribute to fix this!',
    )
  invariant(!description.lokiFilter, 'unsafeLokiFilter not supported with SQLite')

  let sql =
    (!description.eagerJoinTables.length
      ? encodeMethod(table, countMode, hasToManyJoins)
      : encodeEagerMethod(table, countMode, associations, schema)) +
    encodeJoin(description, associations) +
    encodeConditions(table, description, associations) +
    encodeOrderBy(table, description.sortBy) +
    encodeLimitOffset(description.take, description.skip)

  if (description.cte) {
    sql = encodeCTE(description, sql, table)
  }

  return sql
}

export default encodeQuery
