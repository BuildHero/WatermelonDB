/* eslint-disable global-require */

// @ts-ignore
import { NativeModules, TurboModuleRegistry } from 'react-native'

import { fromPairs } from 'rambdax'

import { ConnectionTag, logger, invariant } from '../../../utils/common'

import { fromPromise } from '../../../utils/fp/Result'

import type {
  DispatcherType,
  SQLiteAdapterOptions,
  NativeDispatcher,
  NativeBridgeType,
} from '../type'

import { syncReturnToResult } from '../common'

// Local type definition for the Turbo Module
type NativeWatermelonDBModuleSpec = {
  query(tag: number, table: string, query: string): Record<string, any>[]
  execSqlQuery(tag: number, sql: string, args: Record<string, any>[]): Record<string, any>[]
}

// @ts-ignore
const {
  DatabaseBridge,
}: {
  DatabaseBridge: NativeBridgeType
} = NativeModules

export { DatabaseBridge }

// Try to get the Turbo Module if available
let NativeWatermelonDBModule: NativeWatermelonDBModuleSpec | null = null
try {
  // @ts-ignore
  NativeWatermelonDBModule = TurboModuleRegistry.get<NativeWatermelonDBModuleSpec>(
    'NativeWatermelonDBModule',
  )
  if (NativeWatermelonDBModule) {
    logger.log('[WatermelonDB][SQLite] Turbo Module available, will use for supported methods')
  }
} catch (e) {
  // Turbo Modules not available
  NativeWatermelonDBModule = null
}

const dispatcherMethods = [
  'copyTables',
  'initialize',
  'setUpWithSchema',
  'setUpWithMigrations',
  'find',
  'query',
  'count',
  'batch',
  'batchJSON',
  'getDeletedRecords',
  'destroyDeletedRecords',
  'unsafeResetDatabase',
  'getLocal',
  'setLocal',
  'removeLocal',
  'execSqlQuery',
  'enableNativeCDC',
]

const supportedHybridJSIMethods = new Set(['query', 'execSqlQuery'])
const supportedTurboModuleMethods = new Set(['query', 'execSqlQuery'])

const turboModuleEnabled = () => {
  if (NativeWatermelonDBModule) {
    return true
  }

  return false
}

export const makeDispatcher = (
  type: DispatcherType,
  tag: ConnectionTag,
  dbName: string,
  useHybridJSI?: boolean,
): NativeDispatcher => {
  // @ts-ignore
  const jsiDb = type === 'jsi' && global.nativeWatermelonCreateAdapter(dbName)

  if (useHybridJSI && !turboModuleEnabled()) {
    // initialize legacy JSI bridge
    DatabaseBridge.initializeJSIBridge()
  }

  const methods = dispatcherMethods.map((methodName) => {
    // batchJSON is missing on Android
    // @ts-ignore
    if (!DatabaseBridge[methodName] || (methodName === 'batchJSON' && jsiDb)) {
      return [methodName, undefined]
    }

    const name = type === 'synchronous' ? `${methodName}Synchronous` : methodName

    return [
      methodName,
      (...args: any[]) => {
        const callback = args[args.length - 1]
        const otherArgs = args.slice(0, -1)

        if (jsiDb) {
          try {
            const value =
              methodName === 'query' || methodName === 'count'
                ? jsiDb[methodName](...otherArgs, []) // FIXME: temp workaround
                : jsiDb[methodName](...otherArgs)
            callback({ value })
          } catch (error: any) {
            callback({ error })
          }
          return
        }

        // Use Turbo Module if available for supported methods
        if (NativeWatermelonDBModule && supportedTurboModuleMethods.has(methodName)) {
          try {
            let returnValue: any
            if (methodName === 'query') {
              // For query method: query(tag, table, query)
              const [table, query] = otherArgs
              returnValue = NativeWatermelonDBModule.query(tag, table, query)
            } else if (methodName === 'execSqlQuery') {
              // For execSqlQuery method: execSqlQuery(tag, sql, args)
              const [sql, args] = otherArgs
              returnValue = NativeWatermelonDBModule.execSqlQuery(tag, sql, args)
            }
            callback({
              value: returnValue,
            })
          } catch (error: any) {
            callback({ error })
          }
          return
        }

        if (useHybridJSI && supportedHybridJSIMethods.has(methodName)) {
          try {
            // @ts-ignore
            const returnValue = global.WatermelonDB[methodName](tag, ...otherArgs)

            callback(
              syncReturnToResult({
                status: 'success',
                result: returnValue,
              }),
            )
          } catch (error: any) {
            callback({ error })
          }

          return
        }

        // @ts-ignore
        const returnValue = DatabaseBridge[name](tag, ...otherArgs)

        if (type === 'synchronous') {
          callback(syncReturnToResult(returnValue as any))
        } else {
          fromPromise(returnValue, callback)
        }
      },
    ]
  })

  // @ts-ignore
  const dispatcher: any = fromPairs(methods)
  return dispatcher
}

const initializeJSI = () => {
  // @ts-ignore
  if (global.nativeWatermelonCreateAdapter) {
    return true
  }

  if (DatabaseBridge.initializeJSI) {
    try {
      DatabaseBridge.initializeJSI()
      // @ts-ignore
      return !!global.nativeWatermelonCreateAdapter
    } catch (e: any) {
      logger.error('[WatermelonDB][SQLite] Failed to initialize JSI')
      logger.error(e)
    }
  }

  return false
}

export function getDispatcherType(options: SQLiteAdapterOptions): DispatcherType {
  invariant(
    !(options.synchronous && options.experimentalUseJSI),
    '`synchronous` and `experimentalUseJSI` SQLiteAdapter options are mutually exclusive',
  )

  if (options.synchronous) {
    if (DatabaseBridge.initializeSynchronous) {
      return 'synchronous'
    }

    logger.warn(
      `Synchronous SQLiteAdapter not available… falling back to asynchronous operation. This will happen if you're using remote debugger, and may happen if you forgot to recompile native app after WatermelonDB update`,
    )
  } else if (options.experimentalUseJSI) {
    if (initializeJSI()) {
      return 'jsi'
    }

    logger.warn(
      `JSI SQLiteAdapter not available… falling back to asynchronous operation. This will happen if you're using remote debugger, and may happen if you forgot to recompile native app after WatermelonDB update`,
    )
  }

  return 'asynchronous'
}
