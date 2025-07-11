import { TurboModule, TurboModuleRegistry } from 'react-native'

export interface Spec extends TurboModule {
  query(tag: number, table: string, query: string): Record<string, any>[]
  execSqlQuery(tag: number, sql: string, args: Record<string, any>[]): Record<string, any>[]
}

export default TurboModuleRegistry.getEnforcing<Spec>('NativeWatermelonDBModule')
