import { useState, useCallback } from 'react'
import type { QuerySnippet, SnippetGroup } from '../lib/types'

const SNIPPETS_KEY = 'blazer_snippets'
const GROUPS_KEY   = 'blazer_snippet_groups'

// ── Persistence ───────────────────────────────────────────────────────────────

function loadSnippets(): QuerySnippet[] {
  try {
    const raw = localStorage.getItem(SNIPPETS_KEY)
    return raw ? (JSON.parse(raw) as QuerySnippet[]) : []
  } catch { return [] }
}

function saveSnippets(snippets: QuerySnippet[]): void {
  try { localStorage.setItem(SNIPPETS_KEY, JSON.stringify(snippets)) } catch { /* quota */ }
}

function loadGroups(): SnippetGroup[] {
  try {
    const raw = localStorage.getItem(GROUPS_KEY)
    return raw ? (JSON.parse(raw) as SnippetGroup[]) : []
  } catch { return [] }
}

function saveGroups(groups: SnippetGroup[]): void {
  try { localStorage.setItem(GROUPS_KEY, JSON.stringify(groups)) } catch { /* quota */ }
}

// ── Hook ──────────────────────────────────────────────────────────────────────

export function useSnippets() {
  const [snippets, setSnippets] = useState<QuerySnippet[]>(loadSnippets)
  const [groups,   setGroups  ] = useState<SnippetGroup[]>(loadGroups)

  // ── Snippet operations ────────────────────────────────────────────────────

  const addSnippet = useCallback(
    (snippet: Omit<QuerySnippet, 'id' | 'createdAt'>): QuerySnippet => {
      const newSnippet: QuerySnippet = {
        ...snippet,
        id: `s-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        createdAt: Date.now(),
      }
      setSnippets((prev) => {
        const next = [newSnippet, ...prev]
        saveSnippets(next)
        return next
      })
      return newSnippet
    },
    [],
  )

  const updateSnippet = useCallback(
    (id: string, updates: Partial<Omit<QuerySnippet, 'id' | 'createdAt'>>) => {
      setSnippets((prev) => {
        const next = prev.map((s) => (s.id === id ? { ...s, ...updates } : s))
        saveSnippets(next)
        return next
      })
    },
    [],
  )

  const removeSnippet = useCallback((id: string) => {
    setSnippets((prev) => {
      const next = prev.filter((s) => s.id !== id)
      saveSnippets(next)
      return next
    })
  }, [])

  const clearSnippets = useCallback(() => {
    setSnippets([])
    localStorage.removeItem(SNIPPETS_KEY)
  }, [])

  // ── Group operations ──────────────────────────────────────────────────────

  const addGroup = useCallback((name: string): SnippetGroup => {
    const newGroup: SnippetGroup = {
      id: `g-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      name: name.trim(),
      createdAt: Date.now(),
    }
    setGroups((prev) => {
      const next = [...prev, newGroup]
      saveGroups(next)
      return next
    })
    return newGroup
  }, [])

  const renameGroup = useCallback((id: string, name: string) => {
    setGroups((prev) => {
      const next = prev.map((g) => (g.id === id ? { ...g, name: name.trim() } : g))
      saveGroups(next)
      return next
    })
  }, [])

  /** Delete a group and optionally move its snippets to the Default group. */
  const removeGroup = useCallback((id: string) => {
    // Move all snippets in this group to Default (remove groupId)
    setSnippets((prev) => {
      const next = prev.map((s) => (s.groupId === id ? { ...s, groupId: undefined } : s))
      saveSnippets(next)
      return next
    })
    setGroups((prev) => {
      const next = prev.filter((g) => g.id !== id)
      saveGroups(next)
      return next
    })
  }, [])

  return {
    snippets, addSnippet, updateSnippet, removeSnippet, clearSnippets,
    groups,   addGroup,   renameGroup,   removeGroup,
  }
}
