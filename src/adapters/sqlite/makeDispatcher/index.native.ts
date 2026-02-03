/* eslint-disable global-require */

// @ts-ignore
import { NativeModules, TurboModuleRegistry } from 'react-native'

import { fromPairs } from 'rambdax'

import { ConnectionTag, logger } from '../../../utils/common'

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
  configureSync(configJson: string): void
  startSync(reason: string): void
  getSyncStateJson(): string
  addSyncListener(listener: (eventJson: string) => void): number
  removeSyncListener(listenerId: number): void
  setAuthToken(token: string): void
  clearAuthToken(): void
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

// @ts-ignore
if (NativeWatermelonDBModule) {
  // @ts-ignore
  global.WatermelonDB = NativeWatermelonDBModule
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

export const makeDispatcher = (
  type: DispatcherType,
  tag: ConnectionTag,
  dbName: string,
  useHybridJSI?: boolean,
): NativeDispatcher => {
  const methods = dispatcherMethods.map((methodName) => {
    // batchJSON is missing on Android, and not available when using Hybrid JSI
    // @ts-ignore
    if (!DatabaseBridge[methodName] || (methodName === 'batchJSON' && useHybridJSI)) {
      return [methodName, undefined]
    }

    const name = type === 'synchronous' ? `${methodName}Synchronous` : methodName

    return [
      methodName,
      (...args: any[]) => {
        const callback = args[args.length - 1]
        const otherArgs = args.slice(0, -1)

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

export function getDispatcherType(options: SQLiteAdapterOptions): DispatcherType {
  if (options.synchronous) {
    if (DatabaseBridge.initializeSynchronous) {
      return 'synchronous'
    }

    logger.warn(
      `Synchronous SQLiteAdapter not available… falling back to asynchronous operation. This will happen if you're using remote debugger, and may happen if you forgot to recompile native app after WatermelonDB update`,
    )
  }

  return 'asynchronous'
}
