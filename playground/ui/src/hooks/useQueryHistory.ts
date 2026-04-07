import { useState, useEffect, useCallback } from 'react'
import type { QueryHistoryEntry } from '../lib/types'
import { dbLoadHistory, dbAddHistoryEntry, dbDeleteHistoryEntry, dbClearHistory } from '../lib/db'

export function useQueryHistory() {
  const [history, setHistory] = useState<QueryHistoryEntry[]>([])

  useEffect(() => {
    dbLoadHistory().then(setHistory).catch(console.error)
  }, [])

  const addEntry = useCallback((entry: Omit<QueryHistoryEntry, 'id'>) => {
    const newEntry: QueryHistoryEntry = {
      ...entry,
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    }
    setHistory((prev) => [newEntry, ...prev])
    dbAddHistoryEntry(newEntry).catch(console.error)
  }, [])

  const removeEntry = useCallback((id: string) => {
    setHistory((prev) => prev.filter((e) => e.id !== id))
    dbDeleteHistoryEntry(id).catch(console.error)
  }, [])

  const clearHistory = useCallback(() => {
    setHistory([])
    dbClearHistory().catch(console.error)
  }, [])

  return { history, addEntry, removeEntry, clearHistory }
}
