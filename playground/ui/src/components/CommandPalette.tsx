import React, { useState, useEffect, useRef, useMemo } from 'react'
import type { LeftTab } from '../lib/types'

// ── Platform key symbol ────────────────────────────────────────────────────────
const isMac = typeof navigator !== 'undefined' && /Mac/.test(navigator.userAgent)
const M = isMac ? '⌘' : 'Ctrl+'

// ── Tiny SVG icon helper ───────────────────────────────────────────────────────
function Ico({ d, d2, circle, rect }: {
  d?: string | string[]
  d2?: string
  circle?: string
  rect?: string
}) {
  const paths = Array.isArray(d) ? d : d ? [d] : []
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
      fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
      className="shrink-0">
      {paths.map((p, i) => <path key={i} d={p} />)}
      {d2 && <path d={d2} />}
      {circle && <circle cx={circle.split(',')[0]} cy={circle.split(',')[1]} r={circle.split(',')[2]} />}
      {rect && (() => { const [x, y, w, h, rx] = rect.split(','); return <rect x={x} y={y} width={w} height={h} rx={rx} /> })()}
    </svg>
  )
}

// ── Command definition ─────────────────────────────────────────────────────────
interface Cmd {
  id: string
  label: string
  description?: string
  shortcut?: string
  icon: React.ReactNode
  action: () => void
  group: 'Navigate' | 'View' | 'Actions'
}

// ── Props ──────────────────────────────────────────────────────────────────────
interface Props {
  onClose: () => void
  onGoToTab: (tab: LeftTab) => void
  onToggleSettings: () => void
  onToggleResultPane: () => void
  onToggleTheme: () => void
  onToggleAutoRun: () => void
  onClearMessages: () => void
  resultPaneOpen: boolean
  autoRun: boolean
  currentTab: LeftTab
  preference: 'light' | 'dark' | 'system'
}

// ── Keyboard shortcut badge ────────────────────────────────────────────────────
function KbdBadge({ keys }: { keys: string }) {
  return (
    <span className="ml-auto shrink-0 flex items-center gap-0.5">
      {keys.split(' ').map((k, i) => (
        <kbd key={i}
          className="inline-flex items-center justify-center px-1.5 py-0.5 text-[10px] font-mono font-medium
            bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400
            border border-gray-200 dark:border-gray-700 rounded">
          {k}
        </kbd>
      ))}
    </span>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────
export function CommandPalette({
  onClose,
  onGoToTab,
  onToggleSettings,
  onToggleResultPane,
  onToggleTheme,
  onToggleAutoRun,
  onClearMessages,
  resultPaneOpen,
  autoRun,
  currentTab,
  preference,
}: Props) {
  const [query, setQuery] = useState('')
  const [activeIdx, setActiveIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus()
  }, [])

  // Build the theme label for the toggle command
  const themeNextLabel =
    preference === 'light' ? 'Switch to Dark Mode' :
    preference === 'dark'  ? 'Switch to System Mode' :
                             'Switch to Light Mode'
  const themeIcon =
    preference === 'light' ? (
      <Ico d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    ) : preference === 'dark' ? (
      <Ico d={['M2 3h20', 'M2 21h20']} rect="2,3,20,14,2" />
    ) : (
      <Ico d={[
        'M12 1v2', 'M12 21v2', 'M4.22 4.22l1.42 1.42', 'M18.36 18.36l1.42 1.42',
        'M1 12h2', 'M21 12h2', 'M4.22 19.78l1.42-1.42', 'M18.36 5.64l1.42-1.42',
      ]} circle="12,12,5" />
    )

  // Command definitions
  const commands: Cmd[] = useMemo(() => [
    // ── Navigate ─────────────────────────────────────────────────────────────
    {
      id: 'tab-chat',
      label: 'Go to AI Chat',
      description: 'Open the chat tab',
      shortcut: `${M}1`,
      group: 'Navigate' as const,
      icon: <Ico d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />,
      action: () => { onGoToTab('chat'); onClose() },
    },
    {
      id: 'tab-console',
      label: 'Go to Console',
      description: 'Open the SQL console',
      shortcut: `${M}2`,
      group: 'Navigate' as const,
      icon: <Ico d={['M4 17L10 11 4 5', 'M12 19L20 19']} />,
      action: () => { onGoToTab('console'); onClose() },
    },
    {
      id: 'tab-history',
      label: 'Go to History',
      description: 'Browse past queries',
      shortcut: `${M}3`,
      group: 'Navigate' as const,
      icon: <Ico d={['M12 6v6l4 2']} circle="12,12,10" />,
      action: () => { onGoToTab('history'); onClose() },
    },
    {
      id: 'tab-snippets',
      label: 'Go to Snippets',
      description: 'Browse saved query snippets',
      shortcut: `${M}4`,
      group: 'Navigate' as const,
      icon: <Ico d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z" />,
      action: () => { onGoToTab('snippets'); onClose() },
    },
    {
      id: 'tab-schema',
      label: 'Go to Schema',
      description: 'Explore loaded file schemas',
      shortcut: `${M}5`,
      group: 'Navigate' as const,
      icon: <Ico d={['M12 2C6.48 2 2 4.69 2 8s4.48 6 10 6 10-2.69 10-6-4.48-6-10-6z', 'M2 8v8c0 3.31 4.48 6 10 6s10-2.69 10-6V8', 'M2 12c0 3.31 4.48 6 10 6s10-2.69 10-6']} />,
      action: () => { onGoToTab('schema'); onClose() },
    },

    // ── View ─────────────────────────────────────────────────────────────────
    {
      id: 'toggle-result-pane',
      label: resultPaneOpen ? 'Hide Result Pane' : 'Show Result Pane',
      description: 'Toggle the query results panel',
      shortcut: `${M}\\`,
      group: 'View' as const,
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
          fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          className="shrink-0">
          <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
          <line x1="12" y1="3" x2="12" y2="21"/>
        </svg>
      ),
      action: () => { onToggleResultPane(); onClose() },
    },
    {
      id: 'toggle-theme',
      label: themeNextLabel,
      description: 'Cycle light → dark → system',
      shortcut: `${M}D`,
      group: 'View' as const,
      icon: themeIcon,
      action: () => { onToggleTheme(); onClose() },
    },
    {
      id: 'open-settings',
      label: 'Open Settings',
      description: 'API keys, models, temperature',
      shortcut: `${M},`,
      group: 'View' as const,
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
          fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          className="shrink-0">
          <circle cx="12" cy="12" r="3"/>
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>
        </svg>
      ),
      action: () => { onToggleSettings(); onClose() },
    },

    // ── Actions ───────────────────────────────────────────────────────────────
    {
      id: 'toggle-autorun',
      label: autoRun ? 'Disable Autorun' : 'Enable Autorun',
      description: 'Auto-execute queries after AI responds',
      shortcut: `${M}${isMac ? '⇧' : 'Shift+'}A`,
      group: 'Actions' as const,
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
          fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          className="shrink-0">
          <polygon points="5 3 19 12 5 21 5 3"/>
        </svg>
      ),
      action: () => { onToggleAutoRun(); onClose() },
    },
    {
      id: 'clear-chat',
      label: 'Clear Chat Messages',
      description: 'Remove all messages from the chat',
      group: 'Actions' as const,
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
          fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          className="shrink-0">
          <polyline points="3 6 5 6 21 6"/>
          <path d="M19 6l-1 14H6L5 6"/>
          <path d="M10 11v6M14 11v6"/>
          <path d="M9 6V4h6v2"/>
        </svg>
      ),
      action: () => { onClearMessages(); onClose() },
    },
  // eslint-disable-next-line react-hooks/exhaustive-deps
  ], [resultPaneOpen, autoRun, preference, currentTab])

  // Filter by search query
  const filtered = useMemo(() => {
    if (!query.trim()) return commands
    const q = query.toLowerCase()
    return commands.filter(
      c => c.label.toLowerCase().includes(q) || c.description?.toLowerCase().includes(q),
    )
  }, [commands, query])

  // Reset active index when filtered list changes
  useEffect(() => { setActiveIdx(0) }, [filtered.length])

  // Keyboard navigation inside the palette
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { e.preventDefault(); onClose(); return }
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setActiveIdx(i => Math.min(i + 1, filtered.length - 1))
        return
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault()
        setActiveIdx(i => Math.max(i - 1, 0))
        return
      }
      if (e.key === 'Enter') {
        e.preventDefault()
        filtered[activeIdx]?.action()
        return
      }
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [filtered, activeIdx, onClose])

  // Scroll active item into view
  useEffect(() => {
    const item = listRef.current?.querySelector<HTMLButtonElement>(`[data-idx="${activeIdx}"]`)
    item?.scrollIntoView({ block: 'nearest' })
  }, [activeIdx])

  // Group the filtered commands
  const groups = useMemo(() => {
    const map = new Map<string, Cmd[]>()
    for (const cmd of filtered) {
      const arr = map.get(cmd.group) ?? []
      arr.push(cmd)
      map.set(cmd.group, arr)
    }
    return map
  }, [filtered])

  // Running index across all groups (for activeIdx tracking)
  let runningIdx = 0

  return (
    /* Backdrop */
    <div
      className="fixed inset-0 z-50 flex items-start justify-center pt-[14vh] px-4"
      style={{ background: 'rgba(0,0,0,0.45)' }}
      onMouseDown={(e) => { if (e.target === e.currentTarget) onClose() }}
    >
      {/* Panel */}
      <div
        className="w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl
          border border-gray-200 dark:border-gray-700 overflow-hidden
          flex flex-col"
        style={{ maxHeight: '60vh' }}
        onMouseDown={e => e.stopPropagation()}
      >
        {/* Search input */}
        <div className="flex items-center gap-2.5 px-4 py-3 border-b border-gray-100 dark:border-gray-800">
          <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24"
            fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            className="text-gray-400 dark:text-gray-500 shrink-0">
            <circle cx="11" cy="11" r="8"/>
            <line x1="21" y1="21" x2="16.65" y2="16.65"/>
          </svg>
          <input
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="Search commands…"
            className="flex-1 text-sm bg-transparent outline-none text-gray-900 dark:text-gray-100
              placeholder-gray-400 dark:placeholder-gray-500"
          />
          <kbd className="text-[10px] font-mono text-gray-400 dark:text-gray-500
            bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700
            px-1.5 py-0.5 rounded shrink-0">
            esc
          </kbd>
        </div>

        {/* Command list */}
        <div ref={listRef} className="overflow-y-auto flex-1 py-1.5">
          {filtered.length === 0 ? (
            <div className="flex items-center justify-center py-10 text-sm text-gray-400 dark:text-gray-500">
              No commands found for &ldquo;{query}&rdquo;
            </div>
          ) : (
            Array.from(groups.entries()).map(([groupName, cmds]) => (
              <div key={groupName}>
                {/* Group header */}
                <div className="px-4 pt-2 pb-1">
                  <span className="text-[10px] font-semibold uppercase tracking-widest
                    text-gray-400 dark:text-gray-600">
                    {groupName}
                  </span>
                </div>
                {/* Commands */}
                {cmds.map((cmd) => {
                  const idx = runningIdx++
                  const isActive = idx === activeIdx
                  return (
                    <button
                      key={cmd.id}
                      data-idx={idx}
                      onMouseEnter={() => setActiveIdx(idx)}
                      onClick={cmd.action}
                      className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                        isActive
                          ? 'bg-indigo-50 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300'
                          : 'text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800/60'
                      }`}
                    >
                      {/* Icon */}
                      <span className={`${isActive ? 'text-indigo-500 dark:text-indigo-400' : 'text-gray-400 dark:text-gray-500'}`}>
                        {cmd.icon}
                      </span>

                      {/* Label + description */}
                      <span className="flex flex-col flex-1 min-w-0">
                        <span className="text-sm font-medium leading-tight truncate">
                          {cmd.label}
                        </span>
                        {cmd.description && (
                          <span className={`text-[11px] leading-tight truncate mt-0.5 ${
                            isActive ? 'text-indigo-400 dark:text-indigo-500' : 'text-gray-400 dark:text-gray-500'
                          }`}>
                            {cmd.description}
                          </span>
                        )}
                      </span>

                      {/* Shortcut badge */}
                      {cmd.shortcut && <KbdBadge keys={cmd.shortcut} />}
                    </button>
                  )
                })}
              </div>
            ))
          )}
        </div>

        {/* Footer hint */}
        <div className="shrink-0 flex items-center gap-3 px-4 py-2 border-t border-gray-100 dark:border-gray-800
          text-[11px] text-gray-400 dark:text-gray-600">
          <span className="flex items-center gap-1">
            <kbd className="px-1 py-0.5 rounded bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 font-mono text-[10px]">↑↓</kbd>
            navigate
          </span>
          <span className="flex items-center gap-1">
            <kbd className="px-1 py-0.5 rounded bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 font-mono text-[10px]">↵</kbd>
            run
          </span>
          <span className="flex items-center gap-1">
            <kbd className="px-1 py-0.5 rounded bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 font-mono text-[10px]">esc</kbd>
            close
          </span>
        </div>
      </div>
    </div>
  )
}
