import { useEffect, useRef } from 'react'
import type { LeftTab } from '../lib/types'

export interface ShortcutHandlers {
  onGoToTab: (tab: LeftTab) => void
  onToggleSettings: () => void
  onToggleResultPane: () => void
  onToggleTheme: () => void
  onToggleAutoRun: () => void
  onToggleEngine: () => void
  onClearMessages: () => void
  onOpenPalette: () => void
  isSettingsOpen: boolean
  isPaletteOpen: boolean
}

const TAB_MAP: Record<string, LeftTab> = {
  '1': 'chat',
  '2': 'console',
  '3': 'history',
  '4': 'snippets',
  '5': 'schema',
}

function isEditableTarget(e: KeyboardEvent): boolean {
  const target = e.target as HTMLElement
  if (!target) return false
  const tag = target.tagName
  if (tag === 'INPUT' || tag === 'TEXTAREA') return true
  if (target.isContentEditable) return true
  // CodeMirror editor elements
  if (target.classList.contains('cm-content') || target.classList.contains('cm-line')) return true
  return false
}

/**
 * Global keyboard shortcut handler.
 * Uses a ref-based pattern so the event listener is registered once and always
 * has access to the latest handlers/state without needing to re-register.
 *
 * Shortcuts (Cmd on macOS, Ctrl on Windows/Linux):
 *   Mod+P        → Command palette  (works even inside inputs)
 *   Mod+,        → Settings
 *   Mod+\        → Toggle result pane
 *   Mod+D        → Cycle dark/light/system theme
 *   Mod+E        → Toggle engine on active tab (Blazer ↔ DuckDB)
 *   Mod+1–5      → Switch tabs
 *   Mod+Shift+A  → Toggle autorun
 */
export function useKeyboardShortcuts(handlers: ShortcutHandlers) {
  // Keep a ref so the stable listener always reads the latest values
  const ref = useRef<ShortcutHandlers>(handlers)
  ref.current = handlers

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      const isMod = e.metaKey || e.ctrlKey
      if (!isMod) return

      const h = ref.current
      const key = e.key.toLowerCase()

      // ── Cmd/Ctrl+P → Command palette ─────────────────────────
      // Available everywhere — even inside inputs
      if (key === 'p' && !e.shiftKey) {
        e.preventDefault()
        if (h.isPaletteOpen) {
          // If already open, Escape handles close — but re-pressing Cmd+P should also close
          h.onOpenPalette()
        } else {
          h.onOpenPalette()
        }
        return
      }

      // ── When a modal is open, only Cmd+P passes through ──────
      if (h.isSettingsOpen || h.isPaletteOpen) return

      // ── Cmd/Ctrl+, → Settings ────────────────────────────────
      if (key === ',') {
        e.preventDefault()
        h.onToggleSettings()
        return
      }

      // ── Cmd/Ctrl+\ → Toggle result pane ──────────────────────
      if (key === '\\') {
        e.preventDefault()
        h.onToggleResultPane()
        return
      }

      // ── Remaining shortcuts: skip when typing in editable elements ──
      if (isEditableTarget(e)) return

      // ── Cmd/Ctrl+D → Cycle theme ──────────────────────────────
      if (key === 'd' && !e.shiftKey) {
        e.preventDefault()
        h.onToggleTheme()
        return
      }

      // ── Cmd/Ctrl+E → Toggle engine on active tab ─────────────
      if (key === 'e' && !e.shiftKey) {
        e.preventDefault()
        h.onToggleEngine()
        return
      }

      // ── Cmd/Ctrl+Shift+A → Toggle autorun ────────────────────
      if (key === 'a' && e.shiftKey) {
        e.preventDefault()
        h.onToggleAutoRun()
        return
      }

      // ── Cmd/Ctrl+1–5 → Switch tabs ───────────────────────────
      if (TAB_MAP[e.key]) {
        e.preventDefault()
        h.onGoToTab(TAB_MAP[e.key])
        return
      }
    }

    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  }, []) // stable — ref always holds latest handlers
}
