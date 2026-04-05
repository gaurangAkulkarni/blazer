import { useState, useCallback } from 'react'
import type { QueryHistoryEntry } from '../lib/types'

const STORAGE_KEY = 'blazer_query_history'
const MAX_ENTRIES = 100

function loadHistory(): QueryHistoryEntry[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw ? (JSON.parse(raw) as QueryHistoryEntry[]) : []
  } catch {
    return []
  }
}

function saveHistory(entries: QueryHistoryEntry[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(entries))
  } catch {
    // localStorage quota exceeded — skip silently
  }
}

export function useQueryHistory() {
  const [history, setHistory] = useState<QueryHistoryEntry[]>(loadHistory)

  const addEntry = useCallback(
    (entry: Omit<QueryHistoryEntry, 'id'>) => {
      const newEntry: QueryHistoryEntry = {
        ...entry,
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      }
      setHistory((prev) => {
        const next = [newEntry, ...prev].slice(0, MAX_ENTRIES)
        saveHistory(next)
        return next
      })
    },
    [],
  )

  const removeEntry = useCallback((id: string) => {
    setHistory((prev) => {
      const next = prev.filter((e) => e.id !== id)
      saveHistory(next)
      return next
    })
  }, [])

  const clearHistory = useCallback(() => {
    setHistory([])
    localStorage.removeItem(STORAGE_KEY)
  }, [])

  return { history, addEntry, removeEntry, clearHistory }
}
