import { useState, useEffect, useCallback } from 'react'
import { dbGetAppState, dbSetAppState } from '../lib/db'

/**
 * Like useState but backed by SQLite app_state table.
 * Initialises with defaultValue synchronously, then loads from DB on mount.
 */
export function usePersistedState<T>(key: string, defaultValue: T) {
  const [value, setValue] = useState<T>(defaultValue)

  useEffect(() => {
    dbGetAppState<T>(key, defaultValue)
      .then((v) => setValue(v))
      .catch(console.error)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const setPersisted = useCallback(
    (updater: T | ((prev: T) => T)) => {
      setValue((prev) => {
        const next = typeof updater === 'function' ? (updater as (p: T) => T)(prev) : updater
        dbSetAppState(key, next).catch(console.error)
        return next
      })
    },
    [key],
  )

  return [value, setPersisted] as const
}
