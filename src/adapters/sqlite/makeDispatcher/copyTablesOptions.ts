import { logger } from '../../../utils/common'

export type CopyTablesMode = 'offThread' | 'blocking'

export type CopyTablesEvent =
  | { type: 'start'; mode: CopyTablesMode; tables: number }
  | { type: 'end'; mode: CopyTablesMode; tables: number; durationMs: number }
  | {
      type: 'error'
      mode: CopyTablesMode
      tables: number
      durationMs: number
      cancelled: boolean
      error: unknown
    }

export type CopyTablesOptions = {
  // false forces the blocking copy on synchronous connections (remote kill switch for the off-thread copy).
  offThread?: boolean
  onEvent?: ((event: CopyTablesEvent) => void) | null
}

const options: { offThread: boolean; onEvent: ((event: CopyTablesEvent) => void) | null } = {
  offThread: true,
  onEvent: null,
}

export function configureCopyTables(next: CopyTablesOptions): void {
  if (next.offThread !== undefined) {
    options.offThread = next.offThread
  }
  if (next.onEvent !== undefined) {
    options.onEvent = next.onEvent
  }
}

export function isCopyTablesOffThreadEnabled(): boolean {
  return options.offThread
}

export function emitCopyTablesEvent(event: CopyTablesEvent): void {
  if (!options.onEvent) {
    return
  }
  try {
    options.onEvent(event)
  } catch (error) {
    logger.warn('[WatermelonDB][SQLite] copyTables onEvent handler threw', error)
  }
}

export const isCopyCancelledError = (error: any): boolean =>
  /cancelled/i.test(String(error?.message ?? error))
