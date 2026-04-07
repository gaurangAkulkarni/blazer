import { useState, useEffect, useCallback } from 'react'
import type { QuerySnippet, SnippetGroup } from '../lib/types'
import {
  dbLoadSnippets, dbSaveSnippet, dbDeleteSnippet, dbClearSnippets,
  dbLoadSnippetGroups, dbSaveSnippetGroup, dbDeleteSnippetGroup,
} from '../lib/db'

export function useSnippets() {
  const [snippets, setSnippets] = useState<QuerySnippet[]>([])
  const [groups,   setGroups  ] = useState<SnippetGroup[]>([])

  useEffect(() => {
    dbLoadSnippets().then(setSnippets).catch(console.error)
    dbLoadSnippetGroups().then(setGroups).catch(console.error)
  }, [])

  const addSnippet = useCallback((snippet: Omit<QuerySnippet, 'id' | 'createdAt'>): QuerySnippet => {
    const newSnippet: QuerySnippet = {
      ...snippet,
      id: `s-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      createdAt: Date.now(),
    }
    setSnippets((prev) => [newSnippet, ...prev])
    dbSaveSnippet(newSnippet).catch(console.error)
    return newSnippet
  }, [])

  const updateSnippet = useCallback((id: string, updates: Partial<Omit<QuerySnippet, 'id' | 'createdAt'>>) => {
    setSnippets((prev) => {
      const next = prev.map((s) => (s.id === id ? { ...s, ...updates } : s))
      const updated = next.find((s) => s.id === id)
      if (updated) dbSaveSnippet(updated).catch(console.error)
      return next
    })
  }, [])

  const removeSnippet = useCallback((id: string) => {
    setSnippets((prev) => prev.filter((s) => s.id !== id))
    dbDeleteSnippet(id).catch(console.error)
  }, [])

  const clearSnippets = useCallback(() => {
    setSnippets([])
    dbClearSnippets().catch(console.error)
  }, [])

  const addGroup = useCallback((name: string): SnippetGroup => {
    const newGroup: SnippetGroup = {
      id: `g-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      name: name.trim(),
      createdAt: Date.now(),
    }
    setGroups((prev) => [...prev, newGroup])
    dbSaveSnippetGroup(newGroup).catch(console.error)
    return newGroup
  }, [])

  const renameGroup = useCallback((id: string, name: string) => {
    setGroups((prev) => {
      const next = prev.map((g) => (g.id === id ? { ...g, name: name.trim() } : g))
      const updated = next.find((g) => g.id === id)
      if (updated) dbSaveSnippetGroup(updated).catch(console.error)
      return next
    })
  }, [])

  const removeGroup = useCallback((id: string) => {
    setSnippets((prev) => {
      const next = prev.map((s) => (s.groupId === id ? { ...s, groupId: undefined } : s))
      next.filter((s) => s.groupId === undefined && prev.find((p) => p.id === s.id)?.groupId === id)
          .forEach((s) => dbSaveSnippet(s).catch(console.error))
      return next
    })
    setGroups((prev) => prev.filter((g) => g.id !== id))
    dbDeleteSnippetGroup(id).catch(console.error)
  }, [])

  return {
    snippets, addSnippet, updateSnippet, removeSnippet, clearSnippets,
    groups,   addGroup,   renameGroup,   removeGroup,
  }
}
